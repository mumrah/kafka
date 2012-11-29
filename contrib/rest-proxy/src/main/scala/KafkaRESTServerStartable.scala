package kafka.rest

import java.util.Properties

import kafka.utils.{Logging, Utils}

class KafkaRESTServerStartable(val config: KafkaRESTServerConfig, val consumerProps: Properties, val producerProps: Properties) extends Logging {

  private var restServer : KafkaRESTServer = null

  init

  private def init() {
    info("Starting up KafkaRESTServer")
    restServer = new KafkaRESTServer(config, consumerProps, producerProps)
  }

  def startup() {
    try {
      restServer.startup()
    }
    catch {
      case e =>
        fatal("Fatal error during KafkaRESTServer startup. Prepare to shutdown", e)
        shutdown()
    }
  }

  def shutdown() {
    try {
      restServer.shutdown()
    }
    catch {
      case e =>
        fatal("Fatal error during KafkaRESTServer shutdown. Prepare to halt", e)
        System.exit(1)
    }
  }

}

object KafkaRESTServerStartableMain extends Logging {
  def main(args: Array[String]): Unit = {
    println(args.length)
    if (args.length != 3) {
      println("USAGE: java [options] %s rest.properties consumer.properties producer.properties".format(classOf[KafkaRESTServer].getSimpleName()))
      System.exit(1)
    }

    try {
      val restProps = Utils.loadProps(args(0))
      val consumerProps = Utils.loadProps(args(1))
      val producerProps = Utils.loadProps(args(2))

      val server = new KafkaRESTServerStartable(new KafkaRESTServerConfig(restProps), consumerProps, producerProps)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          server.shutdown
        }
      })
      server.startup

    } catch {
      case e => fatal(e)
    }
    System.exit(0)
  }
}
