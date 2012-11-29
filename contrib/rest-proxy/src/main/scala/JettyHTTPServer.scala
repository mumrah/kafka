package kafka.rest

import java.io.BufferedReader

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.mortbay.jetty.Request
import org.mortbay.jetty.Server
import org.mortbay.jetty.handler.AbstractHandler
import org.mortbay.jetty.bio.SocketConnector

import kafka.consumer.ConsumerTimeoutException
import kafka.utils.Logging

class JettyHTTPServer(val host: String, val port: Int, val restServer: KafkaRESTServer) extends Server() with Logging {

  val connector : SocketConnector = {
    val _connector : SocketConnector = new SocketConnector()
    _connector.setHost(host)
    _connector.setPort(port)
    _connector
  }
  addConnector(connector)

  val handler : AbstractHandler = new AbstractHandler {
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
      info("request handled")
      req match {
        case req2: Request => req2.setHandled(true)
        case _ => throw new ClassCastException
      }
    }

    /*
     *  Handle an HTTP GET request to http://$host:$port/topic/group
     *
     *  Delegates actually fetching and writing out the message from Kafka to 
     *  the KafkaRESTServer#handle_get_message
     *
     *  Return a 200 plus the message if a message is retrieved from the topic
     *  Return a 408 if the consumer times out
     *  Return a 404 if the request path doesn't have all the required parts (topic/group)
     */
    def handle_get(req: HttpServletRequest, resp: HttpServletResponse) {
      val paths: Array[String] = req.getPathInfo.split("""/""")
      if(paths.length != 3) {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND)
      } else {
        val topic: String = paths(1)
        val group: String = paths(2)
        try {
          info("topic: " + topic + " group: " + group)
          restServer.handle_get_message(topic, group, resp.getOutputStream())
          resp.setStatus(HttpServletResponse.SC_OK)
        } catch {
          case e: ConsumerTimeoutException =>
            resp.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT)
            resp.getOutputStream().write("Timeout reading from Kafka stream".getBytes("UTF-8"))
        }
      }
    }

    /*
     *  Handle an HTTP POST request to http://$host:$port/topic
     *
     *  Delegates actually sending the message to Kafka to KafkaRESTServer#handle_post_message
     *
     *  Return a 204 if successful
     *  Return a 404 if the request path doesn't have all the required parts (topic)
     */
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
        restServer.handle_post_message(topic, body)
        resp.setStatus(HttpServletResponse.SC_NO_CONTENT)
      }
    }

    /*
     *  Handle other HTTP requests (PUT, DELETE, etc)
     *
     *  Return a 405
     */
    def handle_unsupported(req: HttpServletRequest, resp: HttpServletResponse) {
      resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      resp.getWriter().write("Method " + req.getMethod + " is not supported")
    }
  }

  addHandler(handler)
}
