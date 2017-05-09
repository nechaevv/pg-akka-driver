package com.github.nechaevv.postgresql.pool

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Processor, Subscriber, Subscription}

/**
  * Created by CONVPN on 5/7/2017.
  */
class PooledFlowStage[T, R](poolFactory: () => PubSub[T, R], onRelease: PubSub[T, R] => Unit)
  extends GraphStage[FlowShape[T, R]] with LazyLogging {

  val in = Inlet[T]("PooledFlowShape.in")
  val out = Outlet[R]("PooledFlowShape.out")

  override def shape: FlowShape[T, R] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var subscriber: Option[Subscriber[_ >: T]] = None
    var subscription: Option[Subscription] = None
    var requestedItems: Long = 0

    val setSubscriber = getAsyncCallback[Option[Subscriber[_ >: T]]] { subs =>
      subscriber = subs
      tryPushToDownstream()
    }
    val setSubscription = getAsyncCallback[Option[Subscription]] { subs =>
      subscription = subs
      tryPullFromDownstream()
    }

    val pushNext = getAsyncCallback[R](t => push(out, t))
    val pullNext = getAsyncCallback[Long] { n =>
      logger.trace(s"Requested $n items from upstream")
      requestedItems += n - 1
      pull(in)
    }
    val onDownstreamComplete = getAsyncCallback[Unit](_ => completeStage())
    val onDownstreamError = getAsyncCallback[Throwable](ex => failStage(ex))

    val detachDownstream = getAsyncCallback[Unit] { _ =>
      subscription.foreach(_.cancel())
    }

    def tryPushToDownstream(): Unit = if (isAvailable(in)) {
      subscriber foreach { s =>
        s.onNext(grab(in))
        if (requestedItems > 0) {
          requestedItems = requestedItems - 1
          pull(in)
        }
      }
    }

    def tryPullFromDownstream(): Unit = {
      if (isAvailable(out)) subscription.foreach(_.request(1))
    }

    private lazy val downstream = {
      logger.trace("Acquiring connection from pool")
      val ds = poolFactory()
      val poolConnector = new Processor[R, T] {
        //Producer
        override def subscribe(s: Subscriber[_ >: T]): Unit = {
          logger.trace("Downstream subscribed")
          s.onSubscribe(new Subscription {
            override def cancel(): Unit = {
              logger.trace("Downstream subscription cancelled")
              setSubscriber.invoke(None)
              setSubscription.invoke(None)
            }
            override def request(n: Long): Unit = pullNext.invoke(n)
          })
          setSubscriber.invoke(Some(s))
        }
        //Consumer
        override def onError(t: Throwable): Unit = {
          logger.error("Downstream error", t)
          onDownstreamError.invoke(t)
          detachDownstream.invoke()
        }
        override def onComplete(): Unit = {
          logger.trace("Downstream completed")
          onDownstreamComplete.invoke()
          detachDownstream.invoke()
        }
        override def onNext(t: R): Unit = {
          pushNext.invoke(t)
        }
        override def onSubscribe(s: Subscription): Unit = {
          logger.trace("Subscribed to downstream")
          setSubscription.invoke(Some(s))
        }
      }
      ds.pub.subscribe(poolConnector)
      poolConnector.subscribe(ds.sub)
      ds
    }

    setHandlers(in, out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        logger.trace("Push from upstream")
        downstream
        tryPushToDownstream()
      }
      override def onUpstreamFinish(): Unit = {
        logger.trace("Upstream source completed")
        //detachDownstream.invoke()
        //Do nothing
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        logger.error("Upstream failure", ex)
        subscription.foreach(_.cancel())
        failStage(ex)
        onRelease(downstream)
      }

      override def onPull(): Unit = {
        logger.trace("Pull from upstream")
        downstream
        tryPullFromDownstream()
      }
      override def onDownstreamFinish(): Unit = {
        logger.trace("Upstream sink completed")
        subscription.foreach(_.cancel())
        completeStage()
        onRelease(downstream)
      }
    })


  }

}
