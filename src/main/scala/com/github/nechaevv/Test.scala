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

  import akka.stream.scaladsl.GraphDSL.Implicits._

  val address = InetSocketAddress.createUnresolved("localhost", 5432)
  val database = "postgres"
  val user = "postgres"
  val password = "postgres"

  val testCommand = Statement("SELECT datname, encoding, datistemplate, datconnlimit FROM pg_database", Nil)
  //val testCommand = SimpleQuery("SELECT * FROM pg_database")

  /*
  val testFlow = Flow.fromSinkAndSourceMat(
    Sink.seq[HList],
    Source.single(testCommand)
  )(Keep.left)


  val pgMessageFlow = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    val packetParser = builder.add(new PgPacketParser)
    val decode = builder.add(Flow[Packet].map(Decode.apply))
    val encode = builder.add(Flow[FrontendMessage].map(m => m.encode))
    //val msgLog = builder.add(Flow[FrontendMessage].map(m => {logger.trace(m.toString); m }))
    //val outLog = builder.add(Flow[ByteString].map(bs => {logger.trace(s"Out: $bs"); bs }))
    //val inLog = builder.add(Flow[ByteString].map(bs => {logger.trace(s"In: $bs"); bs }))

    val dbConnection = builder.add(Tcp().outgoingConnection(
      remoteAddress = address, halfClose = false))//.buffer(10, OverflowStrategy.backpressure))
    //val dbConnection = builder.add(new TcpConnectionFlow(address))

    //msgLog ~> encode ~> outLog ~> dbConnection ~> inLog ~> packetParser ~> decode
    encode ~> dbConnection ~> packetParser ~> decode

    FlowShape(encode.in, decode.out)
  })

  */

  import com.github.nechaevv.postgresql.marshal.DefaultMarshallers._

  val unmarshaller = implicitly[Unmarshaller[String :: Int :: Boolean :: Int :: HNil]]
/*
  val unmarshalFlow = Flow[CommandResult].collect({
    case ResultRow(values) => unmarshaller(values)
  })
*/
  logger.info("Running")

  /*
  val result = unmarshalFlow.viaMat(testFlow)(Keep.right).joinMat(new ConnectionStage("postgres","postgres",""))(Keep.left)
    .joinMat(pgMessageFlow)(Keep.left).run()
    .recover({
      case NonFatal(ex) =>
        logger.error("Query failed", ex)
        Nil
    })
*/
  val pool = new ConnectionPoolImpl(address, database, user, password, 10)
  val result = pool.run(testCommand).map(rr => unmarshaller(rr.data)).runWith(Sink.seq[HList])
  result flatMap { res =>
    res foreach { cr =>
      logger.info(s"Result: $cr")
    }
    logger.trace("Finishing")
    pool.terminate()
  } foreach { _ =>
    //TimeUnit.SECONDS.sleep(1)
    logger.trace("Stopping AS")
    as.terminate()
  }

}