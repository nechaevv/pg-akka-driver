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

    val setSubscriber = getAsyncCallback[Subscriber[_ >: T]](subs => subscriber = Some(subs))
    val setSubscription = getAsyncCallback[Subscription](subs => subscription = Some(subs))
    val pushNext = getAsyncCallback[R](t => push(out, t))

    private lazy val downstream = {
      val ds = poolFactory()
      val poolConnector = new Processor[R, T] {
        //Producer
        override def subscribe(s: Subscriber[_ >: T]): Unit = {
          setSubscriber.invoke(s)
          if (isAvailable(in)) s.onNext(grab(in))
        }
        //Consumer
        override def onError(t: Throwable): Unit = {
          logger.error("Stage error", t)
          subscription.get.cancel()
        }
        override def onComplete(): Unit = {
          subscription.get.cancel()
        }
        override def onNext(t: R): Unit = pushNext.invoke(t)
        override def onSubscribe(s: Subscription): Unit = {
          setSubscription.invoke(s)
          if (isAvailable(out)) s.request(1)
        }
      }
      ds.pub.subscribe(poolConnector)
      poolConnector.subscribe(ds.sub)
      ds
    }

    setHandlers(in, out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        downstream
        subscriber.foreach(_.onNext(grab(in)))
      }
      override def onPull(): Unit = {
        downstream
        subscription.foreach(_.request(1))
      }
    })
  }

}
