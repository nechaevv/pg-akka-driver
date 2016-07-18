package com.github.nechaevv

import java.security.MessageDigest

import akka.actor.Actor.Receive
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend.{AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationOk, BackendMessage, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.{FrontendMessage, PasswordMessage, StartupMessage}
import com.github.nechaevv.postgresql.protocol.backend
import com.typesafe.scalalogging.LazyLogging

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

  val commandSource = Source.actorRef[FrontendMessage](1000, OverflowStrategy.fail)
  val responseSink = Sink.actorRef[BackendMessage](self, OnCompleted)
  val processFlow = Flow.fromSinkAndSourceMat(Flow[ByteString].via(new PgPacketParser).map(backend.Decode.apply).to(responseSink), commandSource.map(_.encode))(Keep.right)
  val commandListener = Tcp().outgoingConnection("localhost", 5432).joinMat(processFlow)(Keep.right).run()
  commandListener ! StartupMessage(database, user)

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
    case Failure(exception) =>
      logger.error("Processing error", exception)
    case msg => logger.info(s"Received message: $msg")
  }

  def toHex(bytes: Array[Byte]): String = bytes.map(b => "%02x".format(b & 0xFF)).mkString

}

case object OnCompleted
