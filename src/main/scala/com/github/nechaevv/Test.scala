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
import com.github.nechaevv.postgresql.protocol.backend.{AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationOk, BackendMessage, CommandComplete, PgPacketParser, ReadyForQuery}
import com.github.nechaevv.postgresql.protocol.frontend.{FrontendMessage, PasswordMessage, Query, StartupMessage, Terminate}
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
  val processFlow = Flow.fromSinkAndSourceMat(Flow[ByteString].via(new PgPacketParser).map(backend.Decode.apply).to(responseSink), commandSource.map(_.encode))(Keep.right)
  val commandListener = Tcp().outgoingConnection(remoteAddress = new InetSocketAddress("localhost", 5432), halfClose = false).joinMat(processFlow)(Keep.right).run()
  commandListener ! StartupMessage(database, user)

  override def postStop(): Unit = context.system.terminate()

  override def receive: Receive = {
    case AuthenticationCleartextPassword =>
      logger.info("Requested cleartext auth")
      commandListener ! PasswordMessage(password)
    case AuthenticationMD5Password(salt) =>
      logger.info("Requested md5 auth")
      val md = MessageDigest.getInstance("MD5")
      md.update(password.getBytes())
      md.update(user.getBytes())
      md.update(toHex(md.digest()).getBytes())
      md.update(salt)
      val md5pass = "md5" + toHex(md.digest())
      commandListener ! PasswordMessage(md5pass)
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
      } else commandListener ! Query(queries.dequeue())
    case CommandComplete(_) =>
      logger.info("Command complete")

    case msg => logger.info(s"Received message: $msg")
  }

  def toHex(bytes: Array[Byte]): String = bytes.map(b => "%02x".format(b & 0xFF)).mkString

}

case object OnCompleted
