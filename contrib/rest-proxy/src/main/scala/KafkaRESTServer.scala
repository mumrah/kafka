package kafka.rest

import scala.collection.JavaConversions._
import scala.io.Source

import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import kafka.consumer._
import kafka.message._
import kafka.producer._
import kafka.utils.{Logging, Utils}

/*
 * Run an HTTP server that acts as a proxy to Kafka topics
 *
 * One producer is shared among all requests, one KafkaStream is created per topic+group
 * and is held in a ConcurrentMap.
 *
 * Jetty is used to handle HTTP requests, but could easily be replaced
 */
class KafkaRESTServer(val config: KafkaRESTServerConfig, val consumerProps: Properties, val producerProps: Properties)
    extends Logging {

  val producer: Producer[String,String] = new Producer[String,String](new ProducerConfig(producerProps))

  val consumerStreamMap = new ConcurrentHashMap[Tuple2[String, String], Tuple2[ConsumerConnector, KafkaStream[Message]]]

  val jetty: JettyHTTPServer = new JettyHTTPServer(config.host, config.port, this)

  def startup() {
    jetty.start
  }

  def shutdown() {
    info("Shutting down")
    // Shut down Jetty
    jetty.stop
    // Shut down all the ConsumerConnectors
    consumerStreamMap.foreach(t2 => {
      info("shutting down stream " + t2._1)
      t2._2._1.shutdown
    })
    // Shut down the producer
    producer.close
  }

  def handle_get_message(topic: String, group: String, output: OutputStream) {
    val streamTuple: Tuple2[ConsumerConnector, KafkaStream[Message]] = openKafkaStream(group, topic) 
    val it = streamTuple._2.iterator
    // Since a KafkaStream may be accessed by multiple threads, and is not itself thread-safe, we must
    // synchronize when reading from it
    val message: Message = {
      streamTuple.synchronized {
        streamTuple._2.head.message
      }
    }
    val payload = message.payload
    output.write(payload.array, payload.arrayOffset, payload.limit)
  }

  def handle_post_message(topic: String, body: String) {
    val data: ProducerData[String,String] = new ProducerData[String, String](topic, body)
    producer.send(data)
  }

  /*
   * Open a new, or return an existing KafkaStream.
   *
   * Atomically get or create a KafkaStream. This method is thread-safe
   */
  def openKafkaStream(group: String, topic: String): Tuple2[ConsumerConnector, KafkaStream[Message]] = {
    val key = new Tuple2(group, topic)
    val connectorAndStream = consumerStreamMap.get(key)
    if(connectorAndStream == null) {
      val _consumerProps = new Properties
      _consumerProps.putAll(consumerProps)
      _consumerProps.put("groupid", group);
      val config = new ConsumerConfig(_consumerProps)
      val connector = Consumer.create(config)
      val stream = connector.createMessageStreams(Map(topic -> 1)).get(topic).get(0)
      val consumerStreamTuple = new Tuple2(connector, stream)
      val maybeInMap = consumerStreamMap.putIfAbsent(key, consumerStreamTuple)
      if(maybeInMap == null) {
        info("opened stream for " + key)
        consumerStreamTuple
      } else { 
        // another KafkaStream was already in the map, return the existing one and shutdown this one
        warn("race opening streams, shutting down this one")
        connector.shutdown
        maybeInMap
      }
    } else {
      debug("got existing stream for " + key)
      connectorAndStream
    }
  }
}

class KafkaRESTServerConfig(props: Properties) {
  val port = Utils.getInt(props, "rest.port", 8080)
  val host = Utils.getString(props, "rest.host", "localhost")
}
