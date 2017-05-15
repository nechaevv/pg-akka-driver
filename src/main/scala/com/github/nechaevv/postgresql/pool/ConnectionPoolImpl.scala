package com.github.nechaevv.postgresql.pool

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Tcp}
import com.github.nechaevv.postgresql.connection._
import com.github.nechaevv.postgresql.protocol.backend.{Decode, Packet, PgPacketParser}
import com.github.nechaevv.postgresql.protocol.frontend.FrontendMessage
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Processor, Publisher, Subscriber, Subscription}

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by CONVPN on 5/5/2017.
  */
class ConnectionPoolImpl(address: InetSocketAddress, database: String, user: String, password: String, minPoolSize:Int, maxPoolSize: Int)
                        (implicit val as: ActorSystem, mat: Materializer, ec: ExecutionContext) extends ConnectionPool with LazyLogging {

  type ConnectionHandle = Processor[SqlCommand, CommandResult]
  val freeConnections: AtomicReference[List[ConnectionHandle]] = new AtomicReference[List[ConnectionHandle]](Nil)

  override def run(cmd: SqlCommand): Source[ResultRow, NotUsed] = Source.single(cmd).via({
    new UpstreamEndpointStage[SqlCommand, CommandResult](getFreeConnection, conn => freeConnections.updateAndGet(conns => conn :: conns))
  }).takeWhile(cr => cr.isInstanceOf[ResultRow], inclusive = true).collect({
    case rr: ResultRow => rr
    case CommandFailed(code, message, detail) => {
      logger.error(message)
      throw new RuntimeException(s"Query failed: $code - $message")
    }
  })

  override def terminate(): Future[Unit] = Future.sequence(freeConnections.get().map(stopConnection)).map(_ => ())

  private def stopConnection(handle: ConnectionHandle): Future[Unit] = {
    val promise = Promise[Unit]
    handle.subscribe(new Subscriber[CommandResult] {
      override def onError(t: Throwable): Unit = {
        logger.error("Connection stop failure", t)
        promise.success()
      }
      override def onComplete(): Unit = {
        logger.trace("Connection stopped")
        promise.success()
      }
      override def onNext(t: CommandResult): Unit = ()
      override def onSubscribe(s: Subscription): Unit = ()
    })
    handle.onComplete()
    promise.future
  }

  private def connectionGraph = {
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val connectionHandleFlow = Flow.fromGraph(new DownstreamEndpointStage[CommandResult, SqlCommand])
    val pgMessageFlow = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val packetParser = builder.add(new PgPacketParser)
      val decode = builder.add(Flow[Packet].map(Decode.apply))
      val encode = builder.add(Flow[FrontendMessage].map(m => m.encode))
      val dbConnection = builder.add(Tcp().outgoingConnection(remoteAddress = address, halfClose = false))

      encode ~> dbConnection ~> packetParser ~> decode

      FlowShape(encode.in, decode.out)
    })

    connectionHandleFlow.joinMat(new ConnectionStage(database, user, password))(Keep.left).joinMat(pgMessageFlow)(Keep.left)
  }

  private def getFreeConnection(): Future[ConnectionHandle] = freeConnections
    .getAndUpdate(conns => conns.drop(1)) match {
    case conn :: _ => Future.successful(conn)
    case Nil =>
      logger.debug(s"Starting new connection")
      connectionGraph.run()
  }

}
