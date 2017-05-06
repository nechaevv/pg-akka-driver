package com.github.nechaevv.stream

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by convpn on 5/4/2017.
  * Alternative implementation of Tcp().outgoingConnection(...)
  */
class TcpConnectionFlow(address: InetSocketAddress)(implicit as: ActorSystem) extends GraphStage[FlowShape[ByteString, ByteString]] with LazyLogging {
  val in = Inlet[ByteString]("TcpConnection.in")
  val out = Outlet[ByteString]("TcpConnection.out")
  override def shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandlers(in, out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        val data = grab(in)
        logger.trace(s"Sending $data")
        tcpActor ! Data(data)
      }

      override def onPull(): Unit = {
        logger.trace("Pulling TCP")
        tcpActor ! PullData
      }
    })

    val tcpActor = as.actorOf(Props(TcpActor.apply(address, getAsyncCallback[TcpCommand] {
      case PullData =>
        logger.trace("Pulling input")
        pull(in)
      case Data(data) =>
        logger.trace(s"Output $data")
        push(out, data)
    } )))

  }

}

class TcpActor(address: InetSocketAddress, callback: AsyncCallback[TcpCommand]) extends Actor with LazyLogging {
  implicit val as = context.system
  IO(Tcp) ! Connect(address, pullMode = true)

  override def receive: Receive = {
    case CommandFailed(cmd) =>
      logger.error(s"TCP Command failed: $cmd")
      context stop self
    case Connected(remote, local) =>
      logger.info(s"Connected to $remote")
      val conn = sender()
      conn ! Register(self)
      context become {
        case Data(data) =>
          logger.trace(s"Sending $data")
          conn ! Write(data, Ack)
        case PullData =>
          logger.trace("Pulling incoming data")
          conn ! ResumeReading
        case Received(data) =>
          logger.trace(s"Received $data")
          callback invoke Data(data)
        case Ack =>
          logger.trace(s"Pulling outgoing data")
          callback invoke PullData
      }
      callback invoke PullData
  }
}

trait TcpCommand
case object PullData extends TcpCommand
case class Data(data: ByteString) extends TcpCommand
case object Ack extends Event

object TcpActor {
  def apply(address: InetSocketAddress, callback: AsyncCallback[TcpCommand]): TcpActor = new TcpActor(address, callback)
}