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

import kafka.network.SocketServer
import kafka.test.annotation.{ClusterTest, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.{ClusterConfig, ClusterInstance}
import org.apache.kafka.common.message.InitProducerIdRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{InitProducerIdRequest, InitProducerIdResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

import java.util.stream.{Collectors, IntStream}
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class ProducerIdsIntegrationTest {

  @BeforeEach
  def setup(clusterConfig: ClusterConfig): Unit = {
    clusterConfig.serverProperties().put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    clusterConfig.serverProperties().put(KafkaConfig.TransactionsTopicReplicationFactorProp, "3")
  }

  @ClusterTest(clusterType = Type.BOTH, brokers = 3)
  def testNonOverlapping(clusterInstance: ClusterInstance): Unit = {
    val ids = clusterInstance.brokerSocketServers().stream().flatMap( broker => {
      IntStream.range(0, 1001).parallel().mapToObj( _ => nextProducerId(broker, clusterInstance.clientListener()))
    }).collect(Collectors.toSet[Long])

    assertEquals(3003, ids.size)

    val expectedIds = Set(0L, 999L, 1000L, 1999L, 2000L, 2999L, 3000L, 4000L, 5000L)
    val idsAsString = ids.asScala.toSet.mkString(", ")
    expectedIds.foreach { id =>
      assertTrue(ids.contains(id), s"Expected to see $id in $idsAsString")
    }
  }

  @ClusterTest(clusterType = Type.ZK, brokers = 1)
  def testNewBlockOnRestart(clusterInstance: ClusterInstance): Unit = {
    val id0 = nextProducerId(clusterInstance.anyBrokerSocketServer(), clusterInstance.clientListener())
    clusterInstance.rollingBrokerRestart()
    val id1 = nextProducerId(clusterInstance.anyBrokerSocketServer(), clusterInstance.clientListener())
    assertEquals(0, id0)
    assertEquals(1000, id1)
  }

  private def nextProducerId(broker: SocketServer, listener: ListenerName): Long = {
    val data = new InitProducerIdRequestData()
      .setProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
      .setProducerId(RecordBatch.NO_PRODUCER_ID)
      .setTransactionalId(null)
      .setTransactionTimeoutMs(10)
    val request = new InitProducerIdRequest.Builder(data).build()

    val response = IntegrationTestUtils.connectAndReceive[InitProducerIdResponse](request,
      destination = broker,
      listenerName = listener)
    response.data().producerId()
  }
}
