package com.github.nechaevv

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, Tcp}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString
import com.github.nechaevv.postgresql.connection._
import com.github.nechaevv.postgresql.marshal.Unmarshaller
import com.github.nechaevv.postgresql.pool.ConnectionPoolImpl
import com.github.nechaevv.postgresql.protocol.backend.{Decode, Packet, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.FrontendMessage
import com.github.nechaevv.stream.TcpConnectionFlow
import com.typesafe.scalalogging.LazyLogging
import slick.collection.heterogeneous.HList
import slick.collection.heterogeneous.syntax._

import scala.util.control.NonFatal

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = as.dispatcher

  val address = InetSocketAddress.createUnresolved("localhost", 5432)
  val database = "postgres"
  val user = "postgres"
  val password = "postgres"

  val q1 = Query("SELECT datname, encoding, datistemplate, datconnlimit FROM pg_database", Nil)

  val testCommand = Statement(q1, Nil, Nil)
  val testCommand2 = Statement(q1, Nil, Seq(1,1,1,1))
  //val testCommand = SimpleQuery("SELECT * FROM pg_database")

  import com.github.nechaevv.postgresql.marshal.DefaultMarshallers._
  logger.info("Running")

  val pool = new ConnectionPoolImpl(address, database, user, password, 1, 10)
  (for {
    result <- pool.run[String :: Int :: Boolean :: Int :: HNil](testCommand).runWith(Sink.seq[HList])
    .map(_ foreach { cr =>
      logger.info(s"Result: $cr")
    })
    result2 <- pool.run[String :: Int :: Boolean :: Int :: HNil](testCommand2).runWith(Sink.seq[HList])
    .map(_  foreach { cr =>
      logger.info(s"Result2: $cr")
    })
  } yield ()).recover({
    case NonFatal(ex) => logger.error("Error", ex)
  }).flatMap(_ => {
    logger.trace("Finishing")
    pool.terminate()
  }).foreach(_ => {
    logger.trace("Stopping AS")
    as.terminate()
  })

}