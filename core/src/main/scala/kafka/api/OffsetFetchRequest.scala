package kafka.api

import java.nio.ByteBuffer

import kafka.api.ApiUtils._
import kafka.common.TopicAndPartition
import kafka.utils.Logging

object OffsetFetchRequest extends Logging {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = "default"

  def readFrom(buffer: ByteBuffer): OffsetFetchRequest = {
    // Read values from the envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetFetchRequest
    val consumerGroupId = readShortString(buffer)
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        TopicAndPartition(topic, partitionId)
      })
    })
    OffsetFetchRequest(consumerGroupId, pairs, versionId, correlationId, clientId)
  }
}

case class OffsetFetchRequest(groupId: String,
                               requestInfo: Seq[TopicAndPartition],
                               versionId: Short = OffsetFetchRequest.CurrentVersion,
                               correlationId: Int = 0,
                               clientId: String = OffsetFetchRequest.DefaultClientId)
    extends RequestOrResponse(Some(RequestKeys.OffsetFetchKey)) {

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_.topic)
  
  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetFetchRequest
    writeShortString(buffer, groupId)             // consumer group
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // (topic, Seq[TopicAndPartition])
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => {
        buffer.putInt(t2.partition)
      })
    })
  }

  override def sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    shortStringLength(groupId) + 
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, t) => {
      count + shortStringLength(t._1) + /* topic */
      4 + /* number of partitions */
      t._2.size * 4 /* partition */
    })
}
