import java.util.concurrent.ConcurrentHashMap

import akka.actor._, akka.io._, akka.util._
import akka.io.Tcp.{Received, Close, CommandFailed, Write}
import scala.collection.mutable
import scala.collection.mutable._
import java.net._


object Server {
  def props(endpoint: InetSocketAddress): Props =
    Props(new Server(endpoint))
}

class Server(endpoint: InetSocketAddress) extends Actor {
  import context.system
  println("Listing on 127.0.0.1:5555")
  IO(Tcp) ! Tcp.Bind(self, endpoint)

  override def receive: Receive = {
    case Tcp.Connected(remote, _) =>
      sender ! Tcp.Register(context.actorOf(Redis.props()))
  }
}

object Main extends App {
  val system = ActorSystem("echo-service-system")
  val endpoint = new InetSocketAddress("localhost", 5555)
  system.actorOf(Server.props(endpoint), "echo-service")

  readLine(s"Hit ENTER to exit ...${System.getProperty("line.separator")}")
  system.shutdown()
}
