package kafka.server
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.message.FetchRequestData
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.{CONSUMER_REPLICA_ID, INVALID_LOG_START_OFFSET}
import org.apache.kafka.storage.internals.log.{FetchIsolation, FetchParams, FetchPartitionData, LogOffsetMetadata}

import java.util
import java.util.Optional
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class ShareFetchReplicaManager(
  val replicaManager: ReplicaManager
) extends ReplicaManagerShim {

  override def readOnePartition(
    topicIdPartition: TopicIdPartition,
    fetchOffset: Long,
    maxFetchSize: Int
  ): FetchPartitionData = {
    val fetchParams = new FetchParams(
      FetchRequestData.HIGHEST_SUPPORTED_VERSION,
      CONSUMER_REPLICA_ID, // Unused in consumer fetch
      -1, // Unused in consumer fetch
      0, // Unused in readFromLog
      0, // Unused in readFromLog
      0, // Unused in readFromLog
      FetchIsolation.HIGH_WATERMARK, // TODO depends on share-group
      Optional.empty) // Unused in share fetch (no fetch from follower)

    val toRead = Seq((topicIdPartition, new FetchRequest.PartitionData(
      topicIdPartition.topicId,
      fetchOffset,
      INVALID_LOG_START_OFFSET, // Only used by replication
      maxFetchSize,
      Optional.empty)))

    val logReadResults = replicaManager.readFromLog(
      fetchParams,
      toRead,
      QuotaFactory.UnboundedQuota,
      readFromPurgatory = true  // Shouldn't matter, but set this true to prevent updating follower state
    )

    logReadResults.head match {
      case (_: TopicIdPartition, logReadResult: LogReadResult) =>
        logReadResult.toFetchPartitionData(false) // Can't do a reassignment fetch from consumer
    }
  }

  override def readManyPartitions(): ReplicaManagerShim.ReadManyBuilder = {
    new ReplicaManagerShim.ReadManyBuilder() {
      private val reads = mutable.Buffer[(TopicIdPartition, FetchRequest.PartitionData)]()

      override def addPartition(topicIdPartition: TopicIdPartition, fetchOffset: Long, maxFetchSize: Int): Unit = {
        reads.append((topicIdPartition, new FetchRequest.PartitionData(
          topicIdPartition.topicId,
          fetchOffset,
          INVALID_LOG_START_OFFSET, // Only used by replication
          maxFetchSize,
          Optional.empty)))
      }

      override def build(): util.Map[TopicIdPartition, FetchPartitionData] = {
        val fetchParams = new FetchParams(
          FetchRequestData.HIGHEST_SUPPORTED_VERSION,
          CONSUMER_REPLICA_ID, // Unused in consumer fetch
          -1, // Unused in consumer fetch
          0, // Unused in readFromLog
          0, // Unused in readFromLog
          0, // Unused in readFromLog
          FetchIsolation.HIGH_WATERMARK, // TODO depends on share-group
          Optional.empty) // Unused in share fetch (no fetch from follower)

        val logReadResults = replicaManager.readFromLog(
          fetchParams,
          reads,
          QuotaFactory.UnboundedQuota,
          readFromPurgatory = true
        )

        logReadResults.map {
          case (partition, result) => partition -> result.toFetchPartitionData(false)
        }.toMap.asJava
      }
    }
  }

  override def addDelayedShareFetch(delayedShareFetch: DelayedShareFetch, keys: util.Collection[DelayedOperationKey]): Boolean = {
    replicaManager.delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, keys.asScala.toSeq)
  }

  override def checkDelayedShareFetch(key: SharePartitionOperationKey): Unit = {
    replicaManager.delayedShareFetchPurgatory.checkAndComplete(key)
  }

  override def getUsableOffset(topicIdPartition: TopicIdPartition, isolation: FetchIsolation): LogOffsetMetadata = {
    val partition = replicaManager.getPartitionOrException(topicIdPartition.topicPartition)
    val offsetSnapshot = partition.fetchOffsetSnapshot(Optional.empty, fetchOnlyFromLeader = true)
    isolation match {
      case FetchIsolation.HIGH_WATERMARK => offsetSnapshot.highWatermark
      case FetchIsolation.TXN_COMMITTED => offsetSnapshot.lastStableOffset
      case FetchIsolation.LOG_END => throw new IllegalArgumentException("Cannot read from the log end with this method")
    }
  }
}
