package com.github.nechaevv

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, Tcp}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString
import com.github.nechaevv.postgresql.connection._
import com.github.nechaevv.postgresql.protocol.backend.{Decode, Packet, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.FrontendMessage
import com.github.nechaevv.stream.TcpConnectionFlow
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = as.dispatcher

  import akka.stream.scaladsl.GraphDSL.Implicits._

  val testCommand = Statement("SELECT * FROM pg_database", Nil)
  //val testCommand = SimpleQuery("SELECT * FROM pg_database")
  val testFlow = Flow.fromSinkAndSourceMat(
    Sink.seq[CommandResult],
    Source.single(testCommand)
  )(Keep.left)

  val address = InetSocketAddress.createUnresolved("localhost", 5432)

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

  logger.info("Running")
  val result = testFlow.joinMat(new ConnectionStage("postgres","postgres",""))(Keep.left)
    .joinMat(pgMessageFlow)(Keep.left).run()

  result foreach {res =>
    res foreach { cr =>
      logger.info(s"Result: $cr")
    }
    as.terminate()
  }

}