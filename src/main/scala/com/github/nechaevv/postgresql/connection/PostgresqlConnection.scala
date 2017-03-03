package com.github.nechaevv.postgresql.connection

import java.net.InetSocketAddress
import java.security.MessageDigest

import akka.NotUsed
import akka.actor.{Actor, ActorRef, FSM, Props, Stash}
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend
import com.github.nechaevv.postgresql.protocol.backend._
import com.github.nechaevv.postgresql.protocol.frontend._
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
class PostgresqlConnection(address: InetSocketAddress, database: String, user: String, password: String)(implicit mat: ActorMaterializer)
  extends Actor with FSM[ConnectionState, ConnectionProperties] with ActorPublisher[FrontendMessage] with LazyLogging {
  private implicit val as = context.system

  override def preStart(): Unit = {
    super.preStart()
    val commandSink = Sink.actorRefWithAck(self, ListenerReady, Ack, ListenerCompleted)
    logger.trace("Starting TCP connection")
    val source = Source.actorPublisher[FrontendMessage](Props(classOf[PgMessagePublisher], mat)).map(_.encode)
    val sink = Flow[ByteString].via(new PgPacketParser).map(backend.Decode.apply).to(commandSink)
    val flow = Flow.fromSinkAndSourceMat(sink, source)(Keep.right)
    val commandPublisher = Tcp().outgoingConnection(remoteAddress = address, halfClose = false)
      .joinMat(flow)(Keep.right).run()
    commandPublisher ! StartupMessage(database, user)
    startWith(StartingUp, ConnectionContext(commandPublisher))
  }

  when(StartingUp) {
    case Event(ListenerReady, _) =>
      sender ! Ack
      stay()
    case Event(AuthenticationCleartextPassword, ConnectionContext(commandPublisher)) =>
      logger.trace("Requested cleartext auth")
      sender ! Ack
      commandPublisher ! PasswordMessage(password)
      goto(Authenticating)
    case Event(AuthenticationMD5Password(salt), ConnectionContext(commandPublisher)) =>
      logger.trace("Requested md5 auth")
      sender ! Ack
      commandPublisher ! PasswordMessage(md5password(user, password, salt))
      goto(Authenticating)
    case Event(AuthenticationOk, _) =>
      logger.trace("Authentication succeeded")
      sender ! Ack
      goto(Authenticated)
  }
  when(Authenticating) {
    case Event(AuthenticationOk, _) =>
      logger.trace("Authentication succeeded")
      sender ! Ack
      goto(Authenticated)
  }
  when(Authenticated) {
    case Event(ParameterStatus(name, value), _) =>
      logger.trace(s"Connection parameter $name=$value")
      sender ! Ack
      stay()
    case Event(BackendKeyData(pid, key), _) =>
      logger.trace(s"Backend key data $pid $key")
      sender ! Ack
      stay()
    case Event(ReadyForQuery(txStatus), _) =>
      logger.trace(s"Ready for query (tx status $txStatus)")
      sender ! Ack
      goto(Ready)
  }
  when(Ready) {
    case Event(cmd: QueryCommand, ConnectionContext(commandPublisher)) =>
      logger.trace(s"Executing query ${cmd.sql}")
      val (oids, values) = cmd.parameters.unzip
      commandPublisher ! Parse("", cmd.sql, oids)
      commandPublisher ! Bind("", "", Seq.fill(cmd.parameters.length)(1), values, Seq.fill(cmd.resultColumnCount)(1))
      commandPublisher ! Execute("", 0)
      commandPublisher ! Sync
      goto(Querying) using QueryContext(commandPublisher, sender)
  }
  when(Querying) {
    case Event(ParseComplete, _) =>
      logger.trace("Parse completed")
      sender ! Ack
      stay()
    case Event(BindComplete, _) =>
      logger.trace("Bind completed")
      sender ! Ack
      stay()
    case Event(dataRow: DataRow, QueryContext(_, queryListener)) =>
      sender ! Ack
      queryListener ! dataRow
      stay()
    case Event(CommandComplete(tag), QueryContext(commandPublisher, queryListener)) =>
      logger.trace(s"Command completed $tag")
      sender ! Ack
      queryListener ! CommandComplete
      stay()
      //goto(Ready) using ConnectionContext(commandPublisher)
    case Event(ReadyForQuery(txStatus), QueryContext(commandPublisher, queryListener)) =>
      logger.trace(s"Ready for query (tx status $txStatus)")
      sender ! Ack
      goto(Ready) using ConnectionContext(commandPublisher)
  }

  whenUnhandled {
    case Event(Ack, _) =>
      stay()
    case Event(event, state) =>
      logger.error(s"Unhandled event $event")
      stop(FSM.Failure(new IllegalStateException()))
  }

  onTermination {
    case StopEvent(_, _, ConnectionContext(commandPublisher)) =>
      logger.trace("Connection terminated")
      commandPublisher ! Terminate
  }


  private def md5password(user: String, password: String, salt: Array[Byte]): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(password.getBytes())
    md.update(user.getBytes())
    md.update(toHex(md.digest()).getBytes())
    md.update(salt)
    "md5" + toHex(md.digest())
  }
  private def toHex(bytes: Array[Byte]): String = bytes.map(b => "%02x".format(b & 0xFF)).mkString

}

class PgMessagePublisher(implicit mat: ActorMaterializer) extends Actor with ActorPublisher[FrontendMessage] with Stash {
  override def receive: Receive = {
    case msg: FrontendMessage => if(totalDemand > 0) {
      onNext(msg)
      sender ! Ack
    } else stash()
    case _: Request => unstashAll()
    case source: Source[FrontendMessage, _] =>
      val sink = Sink.actorRefWithAck(self, ListenerReady, Ack, ListenerCompleted)
      source.runWith(sink)
  }
}

case object ListenerReady
case object Ack
case object ListenerCompleted

sealed trait ConnectionState

case object Initializing extends ConnectionState
case object Connecting extends ConnectionState
case object StartingUp extends ConnectionState
case object Authenticating extends ConnectionState
case object Authenticated extends ConnectionState
case object Ready extends ConnectionState
case object Querying extends ConnectionState

sealed trait ConnectionProperties

case class Uninitialized(commandSink: Sink[Packet, NotUsed]) extends ConnectionProperties
case class ConnectionContext(commandPublisher: ActorRef) extends ConnectionProperties
case class QueryContext(commandPublisher: ActorRef, queryListener: ActorRef) extends ConnectionProperties

case class QueryCommand(sql: String, parameters: Seq[(Int, Option[ByteString])], resultColumnCount: Int)