package kafka.server

import java.io.File
import kafka.utils._
import junit.framework.Assert._
import java.util.{Random, Properties}
import kafka.consumer.SimpleConsumer
import org.junit.{After, Before, Test}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet, Message}
import kafka.zk.ZooKeeperTestHarness
import org.scalatest.junit.JUnit3Suite
import kafka.admin.CreateTopicCommand
import kafka.api.{OffsetCommitRequest, OffsetFetchRequest}
import kafka.utils.TestUtils._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.utils.nonthreadsafe
import kafka.utils.threadsafe
import kafka.utils.ZkUtils
import org.junit.After
import org.junit.Before
import org.junit.Test

class OffsetCommitTest extends JUnit3Suite with ZooKeeperTestHarness {
  val random = new Random() 
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  var simpleConsumer: SimpleConsumer = null
  var time: Time = new MockTime()

  @Before
  override def setUp() {
    super.setUp()
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    time = new MockTime()
    server = TestUtils.createServer(new KafkaConfig(config), time)
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
  }

  @After
  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.rm(logDir)
    super.tearDown()
  }

  @Test
  def testCommitOffsetsForUnknownTopic() {
    val topicAndPartition = TopicAndPartition("offsets-unknown-topic", 0)
    val request = OffsetCommitRequest("test-group", Map(topicAndPartition -> 42L))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                 response.requestInfo.get(topicAndPartition).get)
  }

  @Test
  def testCommitOffsetsSimple() {
    val topic = "offsets-simple"
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, server), 1000)

    val topicAndPartition = TopicAndPartition(topic, 0)
    val request = OffsetCommitRequest("test-group", Map(topicAndPartition -> 42L))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.NoError,
                 response.requestInfo.get(topicAndPartition).get)
  }

  @Test
  def testCommitOffsetsMulti() {
    val topic = "offsets-multi"
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, server), 1000)

    val request = OffsetCommitRequest("test-group", Map(
      TopicAndPartition(topic, 0) -> 42L,
      TopicAndPartition(topic, 1) -> 43L,
      TopicAndPartition("foo", 0) -> 44L
    ))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.NoError,
                 response.requestInfo.get(TopicAndPartition(topic, 0)).get)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                 response.requestInfo.get(TopicAndPartition(topic, 1)).get)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                 response.requestInfo.get(TopicAndPartition("foo", 0)).get)
  }

  @Test
  def testCommitOffsetsForUnknownPartition() {
    val topic = "offsets-unknown-part"
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, server), 1000)

    val request = OffsetCommitRequest("test-group", Map(
      TopicAndPartition(topic, 0) -> 42L,
      TopicAndPartition(topic, 1) -> 43L
    ))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.NoError,
                 response.requestInfo.get(TopicAndPartition(topic, 0)).get)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                 response.requestInfo.get(TopicAndPartition(topic, 1)).get)
  }

  @Test
  def testCommitAndFetchOffsetsSimple() {
    val topic = "offsets-simple"
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, server), 1000)

    val topicAndPartition = TopicAndPartition(topic, 0)
    val request = OffsetCommitRequest("test-group", Map(topicAndPartition -> 42L))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.NoError,
                 response.requestInfo.get(topicAndPartition).get)

    val request1 = OffsetFetchRequest("test-group", Seq(topicAndPartition))
    val response1 = simpleConsumer.fetchOffsets(request1)
    assertEquals(ErrorMapping.NoError, response1.requestInfo.get(topicAndPartition).get._2)
    assertEquals(42L, response1.requestInfo.get(topicAndPartition).get._1)
  }

  @Test
  def testFetchUnknownTopic() {
    val topicAndPartition = TopicAndPartition("foo", 0)
    val request = OffsetFetchRequest("test-group", Seq(topicAndPartition))
    val response = simpleConsumer.fetchOffsets(request)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
      response.requestInfo.get(topicAndPartition).get._2)
  }

  @Test
  def testFetchUnknownPartition() {
    val topic = "offsets-simple"
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic, 0, server), 1000)

    val topicAndPartition = TopicAndPartition(topic, 0)
    val request = OffsetCommitRequest("test-group", Map(topicAndPartition -> 42L))
    val response = simpleConsumer.commitOffsets(request)
    assertEquals(ErrorMapping.NoError,
                 response.requestInfo.get(topicAndPartition).get)

    val request1 = OffsetFetchRequest("test-group", Seq(
      TopicAndPartition(topic, 0),
      TopicAndPartition(topic, 1)
    ))
    val response1 = simpleConsumer.fetchOffsets(request1)
    assertEquals(ErrorMapping.NoError, response1.requestInfo.get(TopicAndPartition(topic, 0)).get._2)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, response1.requestInfo.get(TopicAndPartition(topic, 1)).get._2)
    assertEquals(42L, response1.requestInfo.get(TopicAndPartition(topic, 0)).get._1)
    assertEquals(-1L, response1.requestInfo.get(TopicAndPartition(topic, 1)).get._1)
  }

  @Test
  def testCommitAndFetchOffsetsMulti() {
    val topic1 = "offsets-multi-1"
    val topic2 = "offsets-multi-2"
    val topic3 = "not-a-topic"
    CreateTopicCommand.createTopic(zkClient, topic1, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic1, 0, server), 1000)

    CreateTopicCommand.createTopic(zkClient, topic2, 1, 1, "1")
    waitUntilTrue(() => isLeaderLocalOnBroker(topic2, 0, server), 1000)

    val commitRequest = OffsetCommitRequest("test-group", Map(
      TopicAndPartition(topic1, 0) -> 42L, // existing topic+partition
      TopicAndPartition(topic2, 0) -> 43L, // existing topic+partition
      TopicAndPartition(topic3, 0) -> 44L, // non-existant topic
      TopicAndPartition(topic2, 1) -> 45L  // non-existant partition
    ))
    val commitResponse = simpleConsumer.commitOffsets(commitRequest)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get)
    assertEquals(ErrorMapping.NoError, commitResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
      commitResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
      commitResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get)

    val fetchRequest = OffsetFetchRequest("test-group", Seq(
      TopicAndPartition(topic1, 0),
      TopicAndPartition(topic2, 0),
      TopicAndPartition(topic3, 0),
      TopicAndPartition(topic2, 1)
    ))
    val fetchResponse = simpleConsumer.fetchOffsets(fetchRequest)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get._2)
    assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get._2)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
      fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get._2)
    assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
      fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get._2)

    assertEquals(42L, fetchResponse.requestInfo.get(TopicAndPartition(topic1, 0)).get._1)
    assertEquals(43L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 0)).get._1)
    assertEquals(-1L, fetchResponse.requestInfo.get(TopicAndPartition(topic3, 0)).get._1)
    assertEquals(-1L, fetchResponse.requestInfo.get(TopicAndPartition(topic2, 1)).get._1)
  }

}
