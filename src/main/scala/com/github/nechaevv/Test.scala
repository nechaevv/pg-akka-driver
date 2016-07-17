package com.github.nechaevv

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend.{BackendMessage, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.{FrontendMessage, StartupMessage}
import com.github.nechaevv.postgresql.protocol.backend
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val am = ActorMaterializer()
  val commandSource = Source(List(StartupMessage("trading1", "trading"))).via(Flow[FrontendMessage].map(_.encode))
  val responseSink = Sink.foreach[BackendMessage] { msg =>
    logger.info(s"Received frontend message: $msg")
  }
  val processFlow = Flow.fromSinkAndSource(Flow[ByteString].via(new PgPacketParser).map(backend.Decode.apply).to(responseSink), commandSource)
  logger.info("Connecting")
  val r = Tcp().outgoingConnection("localhost", 5432).join(processFlow).run()
  Await.result(r, 5.seconds)
}
