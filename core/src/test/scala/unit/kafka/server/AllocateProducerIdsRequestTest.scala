package kafka.server

import integration.kafka.server.IntegrationTestUtils
import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import org.apache.kafka.common.message.AllocateProducerIdsRequestData
import org.apache.kafka.common.requests.{AllocateProducerIdsRequest, AllocateProducerIdsResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.extension.ExtendWith

@ClusterTestDefaults(clusterType = Type.BOTH)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class AllocateProducerIdsRequestTest(cluster: ClusterInstance) {
  @ClusterTest
  def testAllocateProducerIds(): Unit = {
    val data = new AllocateProducerIdsRequestData().setBrokerId(1).setBrokerEpoch(0L)
    val request = new AllocateProducerIdsRequest.Builder(data).build()
    val response = IntegrationTestUtils.connectAndReceive[AllocateProducerIdsResponse](request,
      destination = cluster.anyControllerSocketServer(),
      listenerName = cluster.clientListener())
    assertEquals(response.data().producerIdStart(), 0)
    assertEquals(response.data().producerIdLen(), 1000)
  }
}
