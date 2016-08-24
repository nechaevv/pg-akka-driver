package com.github.nechaevv

import java.net.InetSocketAddress
import java.security.MessageDigest

import akka.actor.Actor.Receive
import akka.actor.Status.{Failure, Success}
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend.{AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationOk, BackendMessage, BindComplete, CommandComplete, DataRow, ParseComplete, PgPacketParser, ReadyForQuery}
import com.github.nechaevv.postgresql.protocol.frontend.{Bind, Execute, FrontendMessage, Parse, PasswordMessage, Query, StartupMessage, Sync, Terminate}
import com.github.nechaevv.postgresql.protocol.backend
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  logger.info("Connecting")
  as.actorOf(Props[TestActor])
}

class TestActor extends Actor with LazyLogging {
  val database = "trading"
  val user = "trading"
  val password = "trading"

  implicit val as = context.system
  implicit val am = ActorMaterializer()
  val queries = mutable.Queue("SELECT * FROM \"TEST\"")

  val commandSource = Source.actorRef[FrontendMessage](1000, OverflowStrategy.fail)
  val responseSink = Sink.actorRef[BackendMessage](self, OnCompleted)
  val processFlow = Flow.fromSinkAndSourceMat(Flow[ByteString].via(new PgPacketParser).map(logMessage("received")).map(backend.Decode.apply).to(responseSink), commandSource.map(_.encode).map(logMessage("sent")))(Keep.right)
  val commandListener = Tcp().outgoingConnection(remoteAddress = new InetSocketAddress("localhost", 5432), halfClose = false).joinMat(processFlow)(Keep.right).run()
  commandListener ! StartupMessage(database, user)

  override def postStop(): Unit = context.system.terminate()

  override def receive: Receive = {
    case AuthenticationCleartextPassword =>
      logger.info("Requested cleartext auth")
      commandListener ! PasswordMessage(password)
    case AuthenticationMD5Password(salt) =>
      logger.info("Requested md5 auth")
      commandListener ! PasswordMessage(md5password(user, password, salt))
    case AuthenticationOk =>
      logger.info("Authentication succeeded")
    case OnCompleted =>
      logger.info("Connection closed")
      self ! PoisonPill
    case Failure(exception) =>
      logger.error("Processing error", exception)
    case ReadyForQuery(_) =>
      logger.info("Ready for query")
      if (queries.isEmpty) {
        commandListener ! Terminate
        commandListener ! Success(())
      } else {
        //commandListener ! Query(queries.dequeue())
        logger.info("Parse")
        commandListener ! Parse("", queries.dequeue(), Nil)
        logger.info("Bind")
        commandListener ! Bind("", "", Nil, Nil, Nil)
        logger.info("Execute")
        commandListener ! Execute("", 0)
        logger.info("Sync")
        commandListener ! Sync
        context.become(queryReceive, discardOld = false)
      }
    case CommandComplete(_) =>
      logger.info("Command completed")

    case msg => logger.info(s"Received message: $msg")
  }

  def queryReceive: Receive = {
      case ParseComplete =>
        logger.info("Parse completed")
      case BindComplete =>
        logger.info("Bind completed")
      case CommandComplete(_) =>
        logger.info("Command completed")
        context.unbecome()
      case DataRow(values) =>
        logger.info("DataRow: " + values.map(_.map(b => new String(b.toArray))).mkString(","))
      case msg => logger.info(s"Received message: $msg")
  }

  def md5password(user: String, password: String, salt: Array[Byte]): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(password.getBytes())
    md.update(user.getBytes())
    md.update(toHex(md.digest()).getBytes())
    md.update(salt)
    "md5" + toHex(md.digest())
  }

  def toHex(bytes: Array[Byte]): String = bytes.map(b => "%02x".format(b & 0xFF)).mkString

  def logMessage[T](kind: String)(msg: T):T = {
    logger.trace(s"$kind message: " + msg.toString)
    msg
  }

}

case object OnCompleted
