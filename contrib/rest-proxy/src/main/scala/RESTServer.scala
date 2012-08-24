package kafka.rest

import scala.collection.JavaConversions._
import scala.io.Source

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import java.io.BufferedReader
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.mortbay.jetty.Request
import org.mortbay.jetty.Server
import org.mortbay.jetty.handler.AbstractHandler

import kafka.consumer._
import kafka.message._
import kafka.producer._
import kafka.utils.Logging

class RESTServer(port: Int, zkConnect: String) extends Server(port) with Logging {

  val props: Properties = new Properties()
  props.put("zk.connect", zkConnect)
  
  val producer: Producer[String,String] = {
    val producerProps = new Properties()
    producerProps.putAll(props)
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder")
    new Producer[String,String](new ProducerConfig(producerProps))
  }

  addHandler(new RESTHandler)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run() {
      shutdown
    }
  })

  class RESTHandler extends AbstractHandler { 
    override def handle(target: String, req: HttpServletRequest, resp: HttpServletResponse, dispatch: Int) {
      info("handling request")
      val method_handler = {
        req.getMethod match {
            case "GET" => handle_get _
            case "POST" => handle_post _
            case _ => handle_unsupported _
        }
      }
      method_handler(req, resp)
      req match {
        case req2: Request => req2.setHandled(true)
        case _ => throw new ClassCastException
      }
    }
  }

  def handle_get(req: HttpServletRequest, resp: HttpServletResponse) {
    val paths: Array[String] = req.getPathInfo.split("""/""")
    if(paths.length != 3) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
    } else {
      val topic: String = paths(1)
      val group: String = paths(2)
      // Only open up one thread per group+topic, this means we need to sync when reading from the stream
      val streamTuple: Tuple2[ConsumerConnector, KafkaStream[Message]] = openKafkaStream(group, topic) 
      val it = streamTuple._2.iterator
      try {
        val message: Message = {
          streamTuple.synchronized {
            streamTuple._2.head.message
          }
        }
        val payload = message.payload
        resp.getOutputStream().write(payload.array, payload.arrayOffset, payload.limit)
        resp.setStatus(HttpServletResponse.SC_OK)
      } catch {
        case e: ConsumerTimeoutException =>
          resp.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT)
          resp.getWriter().write("Timeout reading from Kafka stream")
      }
    }
  }

  def handle_post(req: HttpServletRequest, resp: HttpServletResponse) {
    val paths: Array[String] = req.getPathInfo.split("""/""")
    if(paths.length != 2) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
    } else {
      val topic: String = paths(1)
      val body = {
        val reader: BufferedReader = req.getReader
        var line = reader.readLine
        var body_ = line
        while(line != null && reader.ready) {
          line = reader.readLine 
          body_ += line
        }
        reader.close
        body_
      }
      val data: ProducerData[String,String] = new ProducerData[String, String](topic, body)
      producer.send(data)
      resp.setStatus(HttpServletResponse.SC_NO_CONTENT)
    }
  }

  def handle_unsupported(req: HttpServletRequest, resp: HttpServletResponse) {
    resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    resp.getWriter().write("Method " + req.getMethod + " is not supported")
  }

  val streamMap = new ConcurrentHashMap[Tuple2[String, String], Tuple2[ConsumerConnector, KafkaStream[Message]]]

  def openKafkaStream(group: String, topic: String): Tuple2[ConsumerConnector, KafkaStream[Message]] = {
    val key = new Tuple2(group, topic)
    val connectorAndStream = streamMap.get(key)
    if(connectorAndStream == null) {
      val consumerProps = new Properties
      consumerProps.putAll(props)
      consumerProps.put("groupid", group);
      consumerProps.put("auto.commit", "true")
      consumerProps.put("consumer.timeout.ms", "1000")
      val config = new ConsumerConfig(consumerProps)
      val connector = Consumer.create(config)
      val stream = connector.createMessageStreams(Map(topic -> 1)).get(topic).get(0)
      val streamTuple = new Tuple2(connector, stream)
      val maybeInMap = streamMap.putIfAbsent(key, streamTuple)
      if(maybeInMap == null) {
        info("opened stream for " + key)
        streamTuple
      } else {
        info("race opening streams, shutting down this one")
        connector.shutdown
        maybeInMap
      }
    } else {
      debug("got existing stream for " + key)
      connectorAndStream
    }
  }

  def shutdown() {
    info("Shutting down")
    super.stop
    streamMap.foreach(t2 => {
      info("shutting down stream " + t2._1)
      t2._2._1.shutdown
    })
    producer.close
  }

}

object RESTServerMain {
  def main(args: Array[String]) {
    val server = new RESTServer(8080, "localhost:2181")
    server.start
  }
}
