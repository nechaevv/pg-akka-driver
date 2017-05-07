package com.github.nechaevv.postgresql.pool

import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Processor, Publisher, Subscriber, Subscription}

/**
  * Created by CONVPN on 5/6/2017.
  */
class PooledProcessor[T, R](poolFactory: () => PubSub[T, R], onRelease: PubSub[T, R] => Unit)
  extends Processor[T, R] with LazyLogging {

  var upstreamSubscription: Option[Subscription] = None
  var upstreamSubscriber: Option[Subscriber[_ >: R]] = None
  var downstreamSubscription: Option[Subscription] = None

  private lazy val downstream = poolFactory()

  private lazy val poolConnector = new Processor[R, T] {
    var poolSubscription: Option[Subscription] = None

    //As Publisher
    override def subscribe(s: Subscriber[_ >: T]): Unit = {

      downstream.sub.onSubscribe(new Subscription {
        override def cancel(): Unit = upstreamSubscription.foreach(_.cancel())

        override def request(n: Long): Unit = upstreamSubscription.get.request(n)
      })
    }

    //As Subscriber
    override def onError(t: Throwable): Unit = upstreamSubscriber.get.onError(t)

    override def onComplete(): Unit = upstreamSubscriber.get.onComplete()

    override def onNext(t: R): Unit = upstreamSubscriber.get.onNext(t)

    override def onSubscribe(s: Subscription): Unit = {
      if (downstreamSubscription.isEmpty) downstreamSubscription = Some(s)
      else throw new IllegalStateException("Double subscription is not allowed")
      upstreamSubscriber.get.onSubscribe(new Subscription {
        override def cancel(): Unit = s.cancel()
        override def request(n: Long): Unit = s.request(n)
      })
    }
  }

  def release(): Unit = {
    downstreamSubscription.foreach(_.cancel())
    upstreamSubscription.foreach(_.cancel())
    onRelease(downstream)
  }

  //As Publisher
  override def subscribe(s: Subscriber[_ >: R]): Unit = {
    upstreamSubscriber = Some(s)
    downstream.pub.subscribe(poolConnector)
  }

  //As Subscriber
  override def onError(t: Throwable): Unit = {
    logger.error("Upstream error", t)
    release()
  }
  override def onComplete(): Unit = release()
  override def onNext(t: T): Unit = downstream.sub.onNext(t)
  override def onSubscribe(s: Subscription): Unit = {
    if (upstreamSubscription.isEmpty) upstreamSubscription = Some(s)
    else throw new IllegalStateException("Double subscription is not allowed")
    poolConnector.subscribe(downstream.sub)
  }

}

case class PubSub[T, R](pub: Publisher[R], sub: Subscriber[T])