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
class ConnectionPoolImpl(address: InetSocketAddress, database: String, user: String, password: String, maxPoolSize:Int)
                        (implicit val as: ActorSystem, mat: Materializer, ec: ExecutionContext) extends ConnectionPool with LazyLogging {

  type ConnectionHandle = PubSub[SqlCommand, CommandResult]
  val freeConnections: AtomicReference[List[ConnectionHandle]] = new AtomicReference[List[ConnectionHandle]](Nil)

  override def run(cmd: SqlCommand): Source[ResultRow, NotUsed] = Source.single(cmd).via({
    new PooledFlowStage[SqlCommand, CommandResult](getFreeConnection, conn => freeConnections.updateAndGet(conns => conn :: conns))
  }).takeWhile(cr => cr != CommandCompleted, inclusive = false).collect({
    case rr: ResultRow => rr
  })

  override def terminate(): Future[Unit] = Future.sequence(freeConnections.get().map(stopConnection)).map(_ => ())

  private def stopConnection(handle: ConnectionHandle): Future[Unit] = {
    val promise = Promise[Unit]
    handle.pub.subscribe(new Subscriber[CommandResult] {
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
    handle.sub.onComplete()
    promise.future
  }

  private def connectionGraph = {
    import akka.stream.scaladsl.GraphDSL.Implicits._

    val connectionHandleFlow = Flow.fromSinkAndSourceMat(
      Sink.asPublisher[CommandResult](fanout = true),
      Source.asSubscriber[SqlCommand]
    )(Keep.both)

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

  private def getFreeConnection(): ConnectionHandle = freeConnections
    .getAndUpdate(conns => conns.drop(1))
    .headOption.getOrElse({
    logger.debug(s"Starting new connection")
    val (pub, sub) = connectionGraph.run()
    PubSub(pub, sub)
  })

  /*
  class SwitchingProcessor extends Processor[SqlCommand, ResultRow] {
    var connectionHandleOpt: Option[ConnectionHandle] = None

    private def connectionHandle: ConnectionHandle = connectionHandleOpt getOrElse {
      logger.debug("Acquiring connection")
      val conn = getFreeConnection()
      connectionHandleOpt = Some(conn)
      conn
    }

    private def releaseConnection() = connectionHandleOpt map { conn =>
      logger.debug("Releasing connection")
      freeConnections.updateAndGet(conns => conn :: conns)
      connectionHandleOpt = None
    }

    override def subscribe(s: Subscriber[_ >: ResultRow]): Unit = connectionHandle.output.subscribe(new Subscriber[CommandResult] {
      private var subscription: Option[Subscription] = None
      override def onError(t: Throwable): Unit = s.onError(t)
      override def onComplete(): Unit = s.onComplete()
      override def onNext(t: CommandResult): Unit = t match {
        case CommandCompleted =>
          logger.trace("Stopping command subscription")
          s.onComplete()
          subscription match {
            case Some(sn) => sn.cancel()
            case None => logger.error("No active subscription")
          }
        case r: ResultRow => s.onNext(r)
      }
      override def onSubscribe(sn: Subscription): Unit = {
        subscription = Some(sn)
        s.onSubscribe(sn)
      }
    })

    override def onError(t: Throwable): Unit = {
      logger.error("Command stream error")
      releaseConnection()
    }

    override def onComplete(): Unit = {
      logger.trace("Command flow completed")
      //connectionHandle.input.onComplete()
      releaseConnection()
    }

    override def onNext(t: SqlCommand): Unit = {
      connectionHandle.input.onNext(t)
    }

    private var subscription: Option[Subscription] = None
    override def onSubscribe(s: Subscription): Unit = {

      connectionHandle.input.onSubscribe(s)
    }
  }
  */

}
