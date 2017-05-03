package com.github.nechaevv.postgresql.connection

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.{ByteString, Timeout}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by vn on 02.04.17.
  */
class TcpStage(address: InetSocketAddress)(implicit as: ActorSystem, ec: ExecutionContext) extends GraphStage[FlowShape[ByteString, ByteString]] with LazyLogging {
  val in = Inlet[ByteString]("TcpStage.in")
  val out = Outlet[ByteString]("TcpStage.out")
  override def shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    class TcpHandlerActor extends Actor {

      var inBuffer: ByteString = ByteString.empty
      var outBuffer: ByteString = ByteString.empty
      var listener: Option[ActorRef] = None

      override def preStart(): Unit = {
        logger.trace("Connecting TCP")
        IO(Tcp) ! Connect(remoteAddress = address)
      }

      override def receive: Receive = {
        case data: ByteString => outBuffer ++= data
        case Connected(remote, local) =>
          logger.trace("Connected")
          val connection = sender
          connection ! Register(self)
          if (outBuffer.nonEmpty) connection ! Write(outBuffer)
          context become {
            case data: ByteString =>
              connection ! Write(data)
            case Received(data) =>
              inBuffer ++= data
              for (l <- listener) {
                l ! inBuffer
                listener = None
              }
            case GetBuffer =>
              if (inBuffer.nonEmpty) {
                sender ! inBuffer
                inBuffer = ByteString.empty
              } else listener = Some(sender)
            case _:ConnectionClosed =>
              self ! PoisonPill
              completeStage()
          }
      }

    }

    private val tcpHandler = as.actorOf(Props(mkTcpHandler))
    case object GetBuffer
    def mkTcpHandler = new TcpHandlerActor

    override def onPush(): Unit = {
      logger.trace("in push")
      tcpHandler ! grab(in)
      pull(in)
    }

    implicit val timeout = Timeout(10.seconds)
    override def onPull(): Unit = {
      logger.trace("out pull")
      if (!hasBeenPulled(in)) pull(in)
      for (buf <- (tcpHandler ? GetBuffer).mapTo[ByteString]) push(out, buf)
    }

    setHandlers(in, out, this)

  }

}
