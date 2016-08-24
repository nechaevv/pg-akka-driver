package com.github.nechaevv.postgresql

import java.net.InetSocketAddress
import java.nio.ByteOrder

import akka.actor.{Actor, ActorRef, ActorSystem, FSM, Props, Stash}
import akka.actor.Actor.Receive
import akka.io.Tcp.{Connect, Connected}
import akka.io.{IO, Tcp}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend.PgPacketParser
import com.github.nechaevv.postgresql.protocol.frontend.{FrontendMessage, StartupMessage}

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
class PostgresqlConnection(address: InetSocketAddress, database: String, user: String, password: String, materializer: ActorMaterializer)
  extends Actor with FSM[ConnectionState, ConnectionProperties] with ActorPublisher[FrontendMessage] {

  startWith(Connecting, Uninitialized)

  val commandPublisher = {
    val source = Source.actorPublisher[FrontendMessage](Props(classOf[PgMessagePublisher], materializer)).map(_.encode)
    val sink = Flow[ByteString].via(new PgPacketParser).to(Sink.actorRefWithAck(self, ListenerReady, AckPacket, ListenerCompleted))
    val flow = Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
    Tcp().outgoingConnection(remoteAddress = address, halfClose = false).joinMat(flow)(Keep.right).run()
  }

  commandPublisher ! StartupMessage(database, user)

  when(Connecting) {
    case Event(Connected(remote, local), Uninitialized) =>
  }
/*
  when(StartingUp) {
    case Event()
    Frontend.decode()
  }
*/
}

class PgMessagePublisher(materializer: ActorMaterializer) extends Actor with ActorPublisher[FrontendMessage] with Stash {
  override def receive: Receive = {
    case msg: FrontendMessage => if(totalDemand > 0) {
      onNext(msg)
      sender ! AckPacket
    } else stash()
    case _: Request => unstashAll()
    case source: Source[FrontendMessage, _] =>
      val sink = Sink.actorRefWithAck(self, ListenerReady, AckPacket, ListenerCompleted)
      source.runWith(sink)
  }
}

case object ListenerReady
case object AckPacket
case object ListenerCompleted

sealed trait ConnectionState

case object Connecting extends ConnectionState
case object StartingUp extends ConnectionState
case object Authorizing extends ConnectionState
case object Ready extends ConnectionState
case object Querying extends ConnectionState

sealed trait ConnectionProperties

case object Uninitialized extends ConnectionProperties
case class Connected(commandListener: ActorRef) extends ConnectionProperties
