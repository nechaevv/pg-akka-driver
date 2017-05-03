package com.github.nechaevv

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source, Tcp}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString
import com.github.nechaevv.postgresql.connection._
import com.github.nechaevv.postgresql.protocol.backend.{Decode, Packet, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.FrontendMessage
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = as.dispatcher

  import akka.stream.scaladsl.GraphDSL.Implicits._

  //val testCommand = Statement("SELECT * FROM \"TEST\"", Nil)
  val testCommand = SimpleQuery("SELECT * FROM \"TEST\"")
  val testFlow = Flow.fromSinkAndSource(
    Sink.foreach[CommandResult](cmd => logger.info(s"Response: $cmd")),
    Source.single(testCommand)
  )

  val address = InetSocketAddress.createUnresolved("localhost", 5432)

  val pgMessageFlow = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    val packetParser = builder.add(new PgPacketParser)
    val decode = builder.add(Flow[Packet].map(Decode.apply))
    val encode = builder.add(Flow[FrontendMessage].map(m => m.encode))
    val msgLog = builder.add(Flow[FrontendMessage].map(m => {logger.trace(m.toString); m }))
    val outLog = builder.add(Flow[ByteString].map(bs => {logger.trace(s"Out: $bs"); bs }))

    val dbConnection = builder.add(Tcp().outgoingConnection(
      remoteAddress = address, halfClose = false).buffer(10, OverflowStrategy.backpressure))

    //val dbConnection = builder.add(new TcpStage(address))

    msgLog ~> encode ~> outLog ~> dbConnection ~> packetParser ~> decode

    FlowShape(msgLog.in, decode.out)
  })
/*
  val testGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    val runner = builder.add(new ConnectionStage("trading","trading","trading"))
    val test = builder.add(testFlow)
    val pg = builder.add(pgMessageFlow)

    testFlow <~> runner <~> pg

    ClosedShape
  })
*/
  logger.info("Running")
  testFlow.join(new ConnectionStage("trading","trading","trading")).join(pgMessageFlow)
    .run()

  /*
  as.actorOf(Props[TestActor1])

  def connectionFactory = new PostgresqlConnection(
  InetSocketAddress.createUnresolved("localhost", 5432),
  "trading","trading","trading"
  )
  */



}
/*
class TestActor1 extends Actor with LazyLogging {
  logger.info("Creating connection")
  private val conn = context.actorOf(Props(Test.connectionFactory))
  conn ! SubscribeTransitionCallBack(self)

  var commands = List(RawSqlCommand("SELECT * FROM \"TEST\"", Nil, 0))

  override def receive: Receive = {
    case Transition(_, _, Ready) =>
      logger.info("Connection ready")
      commands match {
        case command :: rest =>
          conn ! command
          commands = rest
        case _ =>
      }
    case DataRow(fields) => logger.info(s"Data row: $fields")
    case CommandComplete =>
      logger.info("Command complete")
      context.system.terminate()
  }
}
*/

class TestDbConnectionStage extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("in")
  val out = Outlet[ByteString]("out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape)
    with InHandler with OutHandler {

    override def onPush(): Unit = {
      val x = grab(in)
      //x.iterator.getByte match {
        
      //}
    }
    override def onPull(): Unit = {
    }
    setHandlers(in, out, this)
  }
  override def shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)
}