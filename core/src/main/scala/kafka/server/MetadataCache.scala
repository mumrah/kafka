/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock

import scala.collection.{Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import kafka.api._
import kafka.controller.StateChangeLogger
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import kafka.server.metadata.{MetadataBroker, MetadataImage, MetadataImageBuilder, MetadataPartition}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.{Cluster, Node, PartitionInfo, TopicPartition}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{MetadataResponse, UpdateMetadataRequest}


object MetadataCache {
  def removePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                          topic: String, partitionId: Int): Boolean = {
    partitionStates.get(topic).exists { infos =>
      infos.remove(partitionId)
      if (infos.isEmpty) partitionStates.remove(topic)
      true
    }
  }

  def addOrUpdatePartitionInfo(partitionStates: mutable.AnyRefMap[String, mutable.LongMap[UpdateMetadataPartitionState]],
                               topic: String,
                               partitionId: Int,
                               stateInfo: UpdateMetadataPartitionState): Unit = {
    val infos = partitionStates.getOrElseUpdate(topic, mutable.LongMap.empty)
    infos(partitionId) = stateInfo
  }
}

/**
 * The broker's primary metadata cache, which stores information about nodes and partitions.
 * Every broker maintains the same cache, asynchronously.
 *
 * The information we send back to client MetadataRequests comes from this cache.
 *
 * When in ZooKeeper mode, this cache is populated by UpdateMetadataRequest RPCs from
 * the controller.  When in Raft mode, this cache is populated by BrokerMetadataListener.
 *
 *  @param brokerId               The id of the broker.
 */
class MetadataCache(val brokerId: Int) extends Logging {
  this.logIdent = s"[MetadataCache brokerId=${brokerId}] "

  private val lock = new ReentrantLock()

  //this is the cache state. every MetadataImage instance is immutable, and updates (performed under a lock)
  //replace the value with a completely new one. this means reads (which are not under any lock) need to grab
  //the value of this var (into a val) ONCE and retain that read copy for the duration of their operation.
  //multiple reads of this value risk getting different snapshots.
  @volatile private var _currentImage: MetadataImage = new MetadataImage()

  private val stateChangeLogger = new StateChangeLogger(brokerId, inControllerContext = false, None)

  // This method is the main hotspot when it comes to the performance of metadata requests,
  // we should be careful about adding additional logic here. Relatedly, `brokers` is
  // `List[Integer]` instead of `List[Int]` to avoid a collection copy.
  // filterUnavailableEndpoints exists to support v0 MetadataResponses
  private def maybeFilterAliveReplicas(image: MetadataImage,
                                       brokers: java.util.List[Integer],
                                       listenerName: ListenerName,
                                       filterUnavailableEndpoints: Boolean): java.util.List[Integer] = {
    if (!filterUnavailableEndpoints) {
      brokers
    } else {
      val res = new util.ArrayList[Integer](math.min(image.brokers.aliveBrokers().size, brokers.size))
      for (brokerId <- brokers.asScala) {
        if (hasAliveEndpoint(image, brokerId, listenerName))
          res.add(brokerId)
      }
      res
    }
  }

  def currentImage(): MetadataImage = _currentImage

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  // If errorUnavailableListeners=true, return LISTENER_NOT_FOUND if listener is missing on the broker.
  // Otherwise, return LEADER_NOT_AVAILABLE for broker unavailable and missing listener (Metadata response v5 and below).
  private def getPartitionMetadata(image: MetadataImage, topic: String, listenerName: ListenerName, errorUnavailableEndpoints: Boolean,
                                   errorUnavailableListeners: Boolean): Option[Iterator[MetadataResponsePartition]] = {
    val partitionsIterator = image.partitions.topicPartitions(topic)
    if (!partitionsIterator.hasNext) {
      None
    } else {
      Some(partitionsIterator.map { case partition =>
        val filteredReplicas = maybeFilterAliveReplicas(image, partition.replicas,
          listenerName, errorUnavailableEndpoints)
        val filteredIsr = maybeFilterAliveReplicas(image, partition.isr, listenerName,
          errorUnavailableEndpoints)
        val maybeLeader = getAliveEndpoint(image, partition.leaderId, listenerName)
        maybeLeader match {
          case None =>
            val error = if (image.aliveBroker(partition.leaderId).isEmpty) {
              debug(s"Error while fetching metadata for ${partition.toTopicPartition}: leader not available")
              Errors.LEADER_NOT_AVAILABLE
            } else {
              debug(s"Error while fetching metadata for ${partition.toTopicPartition}: listener $listenerName " +
                s"not found on leader ${partition.leaderId}")
              if (errorUnavailableListeners) Errors.LISTENER_NOT_FOUND else Errors.LEADER_NOT_AVAILABLE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partition.partitionIndex)
              .setLeaderId(MetadataResponse.NO_LEADER_ID)
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(partition.offlineReplicas)

          case Some(leader) =>
            val error = if (filteredReplicas.size < partition.replicas.size) {
              debug(s"Error while fetching metadata for ${partition.toTopicPartition}: replica information not available for " +
                s"following brokers ${partition.replicas.asScala.filterNot(filteredReplicas.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else if (filteredIsr.size < partition.isr.size) {
              debug(s"Error while fetching metadata for ${partition.toTopicPartition}: in sync replica information not available for " +
                s"following brokers ${partition.isr.asScala.filterNot(filteredIsr.contains).mkString(",")}")
              Errors.REPLICA_NOT_AVAILABLE
            } else {
              Errors.NONE
            }

            new MetadataResponsePartition()
              .setErrorCode(error.code)
              .setPartitionIndex(partition.partitionIndex)
              .setLeaderId(leader.id())
              .setLeaderEpoch(partition.leaderEpoch)
              .setReplicaNodes(filteredReplicas)
              .setIsrNodes(filteredIsr)
              .setOfflineReplicas(partition.offlineReplicas)
        }
      })
    }
  }

  /**
   * Check whether a broker is alive and has a registered listener matching the provided name.
   * This method was added to avoid unnecessary allocations in [[maybeFilterAliveReplicas]], which is
   * a hotspot in metadata handling.
   */
  private def hasAliveEndpoint(image: MetadataImage, id: Int, listenerName: ListenerName): Boolean = {
    image.brokers.aliveBroker(id).exists(_.endpoints.get(listenerName.value()).isDefined)
  }

  /**
   * Get the endpoint matching the provided listener if the broker is alive. Note that listeners can
   * be added dynamically, so a broker with a missing listener could be a transient error.
   *
   * @return None if broker is not alive or if the broker does not have a listener named `listenerName`.
   */
  private def getAliveEndpoint(image: MetadataImage, id: Int, listenerName: ListenerName): Option[Node] = {
    image.brokers.aliveBroker(id).flatMap(_.endpoints.get(listenerName.value()))
  }

  // errorUnavailableEndpoints exists to support v0 MetadataResponses
  def getTopicMetadata(topics: Set[String],
                       listenerName: ListenerName,
                       errorUnavailableEndpoints: Boolean = false,
                       errorUnavailableListeners: Boolean = false): Seq[MetadataResponseTopic] = {
    val image = _currentImage
    topics.toSeq.flatMap { topic =>
      getPartitionMetadata(image, topic, listenerName, errorUnavailableEndpoints, errorUnavailableListeners).map { partitionMetadata =>
        new MetadataResponseTopic()
          .setErrorCode(Errors.NONE.code)
          .setName(topic)
          .setIsInternal(Topic.isInternal(topic))
          .setPartitions(partitionMetadata.toBuffer.asJava)
      }
    }
  }

  def getAllTopics(): Set[String] = _currentImage.partitions.allTopicNames()

  def getAllPartitions(): Set[TopicPartition] = {
    _currentImage.partitions.allPartitions().map {
      partition => partition.toTopicPartition
    }.toSet
  }

  def getNonExistingTopics(topics: Set[String]): Set[String] = {
    topics.diff(_currentImage.partitions.allTopicNames())
  }

  def getAliveBroker(brokerId: Int): Option[MetadataBroker] = {
    _currentImage.brokers.aliveBroker(brokerId)
  }

  def getAliveBrokers: Seq[MetadataBroker] = {
    _currentImage.brokers.aliveBrokers().asScala
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[UpdateMetadataPartitionState] = {
    _currentImage.partitions.topicPartition(topic, partitionId).map {
      case partition => new UpdateMetadataPartitionState().
        setTopicName(partition.topicName).
        setPartitionIndex(partition.partitionIndex).
        setControllerEpoch(-1). // Controller epoch is not stored in the cache.
        setLeader(partition.leaderId).
        setLeaderEpoch(partition.leaderEpoch).
        setIsr(partition.isr).
        setZkVersion(-1) // ZK version is not stored in the cache.
    }
  }

  def numPartitions(topic: String): Option[Int] = {
    _currentImage.partitions.numTopicPartitions(topic)
  }

  // if the leader is not known, return None;
  // if the leader is known and corresponding node is available, return Some(node)
  // if the leader is known but corresponding node with the listener name is not available, return Some(NO_NODE)
  def getPartitionLeaderEndpoint(topic: String, partitionId: Int, listenerName: ListenerName): Option[Node] = {
    val image = _currentImage
    image.partitions.topicPartition(topic, partitionId).map { partition =>
      image.aliveBroker(partition.leaderId) match {
        case Some(broker) =>
          broker.endpoints.getOrElse(listenerName.value(), Node.noNode)
        case None =>
          Node.noNode
      }
    }
  }

  def getPartitionReplicaEndpoints(tp: TopicPartition, listenerName: ListenerName): Map[Int, Node] = {
    val image = _currentImage
    image.partitions.topicPartition(tp.topic(), tp.partition()).map { partition =>
      partition.replicas.asScala.map(replicaId => replicaId.intValue() -> {
          image.aliveBroker(replicaId) match {
            case Some(broker) =>
              broker.endpoints.getOrElse(listenerName.value(), Node.noNode())
            case None =>
              Node.noNode()
          }}).toMap
        .filter(pair => pair match {
          case (_, node) => !node.isEmpty
        })
    }.getOrElse(Map.empty[Int, Node])
  }

  def getControllerId: Option[Int] = {
    _currentImage.controllerId
  }

  def getClusterMetadata(clusterId: String, listenerName: ListenerName): Cluster = {
    val image = _currentImage
    val nodes = new util.HashMap[Integer, Node]
    image.brokers.aliveBrokers().asScala.foreach {
      case node => if (!node.fenced) {
        node.endpoints.get(listenerName.value()).foreach { nodes.put(node.id, _) }
      }
    }

    def node(id: Integer): Node = {
      Some(nodes.get(id)).getOrElse(Node.noNode())
    }

    val partitionInfos = new util.ArrayList[PartitionInfo]
    val internalTopics = new util.HashSet[String]

    image.partitions.allPartitions().foreach {
      case partition =>
        partitionInfos.add(new PartitionInfo(partition.topicName,
          partition.partitionIndex, node(partition.leaderId),
          partition.replicas.asScala.map(node(_)).toArray,
          partition.isr.asScala.map(node(_)).toArray,
          partition.offlineReplicas.asScala.map(node(_)).toArray))
        if (Topic.isInternal(partition.topicName)) {
          internalTopics.add(partition.topicName)
        }
    }

    new Cluster(clusterId, nodes.values(),
      partitionInfos, Collections.emptySet[String], internalTopics,
      node(Integer.valueOf(image.controllerId.getOrElse(-1))))
  }

  def stateChangeTraceEnabled(): Boolean = {
    stateChangeLogger.isTraceEnabled
  }

  def logStateChangeTrace(str: String): Unit = {
    stateChangeLogger.trace(str)
  }

  // This method returns the deleted TopicPartitions received from UpdateMetadataRequest
  def updateMetadata(correlationId: Int, request: UpdateMetadataRequest): Seq[TopicPartition] = {
    inLock(lock) {
      val image = _currentImage
      val builder = MetadataImageBuilder(brokerId, logger.underlying, image)

      builder.controllerId(if (request.controllerId() < 0) None else Some(request.controllerId()))

      // Compare the new brokers with the existing ones.
      def toMetadataBroker(broker: UpdateMetadataBroker): MetadataBroker = {
        val endpoints = broker.endpoints().asScala.map { case endpoint =>
          endpoint.listener -> new Node(broker.id(), endpoint.host(), endpoint.port(), broker.rack())
        }.toMap
        MetadataBroker(broker.id(), broker.rack(), endpoints, false)
      }
      val found = new util.IdentityHashMap[MetadataBroker, Boolean](image.numAliveBrokers())
      request.liveBrokers().iterator().asScala.foreach { case brokerInfo =>
        val newBroker = toMetadataBroker(brokerInfo)
        image.brokers.get(brokerInfo.id) match {
          case None => builder.brokersBuilder().add(newBroker)
          case Some(existingBroker) =>
            found.put(existingBroker, true)
            if (!existingBroker.equals(newBroker)) {
              builder.brokersBuilder().add(newBroker)
            }
        }
      }
      image.brokers.iterator().foreach { case broker =>
        if (!found.containsKey(broker)) {
          builder.brokersBuilder().remove(broker.id)
        }
      }

      val traceEnabled = stateChangeLogger.isTraceEnabled
      var numDeleted = 0
      var numAdded = 0
      val deleted = mutable.Buffer[TopicPartition]()
      request.partitionStates().iterator().asScala.foreach { case partition =>
        if (partition.leader() == LeaderAndIsr.LeaderDuringDelete) {
          if (traceEnabled) {
            stateChangeLogger.trace(s"Deleted partition ${partition.topicName()}-${partition.partitionIndex()} " +
              "from metadata cache in response to UpdateMetadata request sent by " +
              s"controller ${request.controllerId} epoch ${request.controllerEpoch} " +
              s"with correlation id $correlationId")
          }
          builder.partitionsBuilder().remove(partition.topicName(), partition.partitionIndex())
          deleted += new TopicPartition(partition.topicName(), partition.partitionIndex())
          numDeleted = numDeleted + 1
        } else {
          val prevPartition = builder.partition(partition.topicName(), partition.partitionIndex())
          val newPartition = MetadataPartition(prevPartition, partition)
          if (traceEnabled) {
            stateChangeLogger.trace(s"Cached leader info $newPartition in response to " +
              s"UpdateMetadata request sent by controller $request.controllerId epoch " +
              s"$request.controllerEpoch with correlation id $correlationId")
          }
          builder.partitionsBuilder().set(newPartition)
          numAdded = numAdded + 1
        }
      }
      stateChangeLogger.info(s"Add ${numAdded} partitions and deleted ${numDeleted} " +
        "partitions to the metadata cache in response to UpdateMetadata request sent by " +
        s"controller ${request.controllerId} epoch ${request.controllerEpoch} with " +
        s"correlation id ${correlationId}")

      _currentImage = builder.build()
      deleted
    }
  }

  def contains(topic: String): Boolean = _currentImage.partitions.contains(topic)

  def contains(tp: TopicPartition): Boolean = {
    _currentImage.partitions.topicPartition(tp.topic(), tp.partition()).isDefined
  }

  def setImage(newImage: MetadataImage) = inLock(lock) {
    _currentImage = newImage
  }
}
