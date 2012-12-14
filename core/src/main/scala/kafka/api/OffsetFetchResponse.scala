package kafka.api

import java.nio.ByteBuffer

import kafka.api.ApiUtils._
import kafka.common.TopicAndPartition
import kafka.utils.Logging

object OffsetFetchResponse extends Logging {
  val CurrentVersion = 1.shortValue()
  val DefaultClientId = ""

  def readFrom(buffer: ByteBuffer): OffsetFetchResponse = {
    // Read values from the envelope
    val versionId = buffer.getShort
    val correlationId = buffer.getInt
    val clientId = readShortString(buffer)

    // Read the OffsetResponse 
    val topicCount = buffer.getInt
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer)
      val partitionCount = buffer.getInt
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt
        val offset = buffer.getLong
        val error = buffer.getShort
        (TopicAndPartition(topic, partitionId), (offset, error))
      })
    })
    OffsetFetchResponse(Map(pairs:_*), versionId, correlationId, clientId)
  }
}

case class OffsetFetchResponse(requestInfo: Map[TopicAndPartition, Tuple2[Long, Short]],
                               versionId: Short = OffsetFetchResponse.CurrentVersion,
                               correlationId: Int = 0,
                               clientId: String = OffsetFetchResponse.DefaultClientId)
    extends RequestOrResponse {

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)

  def writeTo(buffer: ByteBuffer) {
    // Write envelope
    buffer.putShort(versionId)
    buffer.putInt(correlationId)
    writeShortString(buffer, clientId)

    // Write OffsetFetchResponse
    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map[TopicAndPartition, Tuple2[Long, Short]]
      writeShortString(buffer, t1._1) // topic
      buffer.putInt(t1._2.size)       // number of partitions for this topic
      t1._2.foreach( t2 => { // TopicAndPartition -> Tuple2[Long, Short]
        buffer.putInt(t2._1.partition)
        buffer.putLong(t2._2._1)
        buffer.putShort(t2._2._2)
      })
    })
  }

  override def sizeInBytes = 
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, t) => {
      count + 
      shortStringLength(t._1) + /* topic */
      4 + /* number of partitions */
      t._2.size * (
        4 + /* partition */
        8 + /* offset */
        2 /* error */
      )
    })
}

