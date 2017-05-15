package com.github.nechaevv.postgresql.pool

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import org.reactivestreams.{Processor, Subscriber, Subscription}

import scala.concurrent.{Future, Promise}

/**
  * Created by convpn on 5/9/2017.
  */
class DownstreamEndpointStage[T, R] extends GraphStageWithMaterializedValue[FlowShape[T, R], Future[Processor[R, T]]]{
  val in = Inlet[T]("DownstreamEndpointStage.in")
  val out = Outlet[R]("DownstreamEndpointShape.out")

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Processor[R, T]]) = {
    var processor = Promise[Processor[R, T]]
    val logic = new GraphStageLogic(shape) {
      var subscriber: Option[Subscriber[_ >: T]] = None
      var subscription: Option[Subscription] = None
      var requested: Long = 0

      val setSubscriber = getAsyncCallback[Option[Subscriber[_ >: T]]] { s =>
        this.subscriber = s
        if (isAvailable(in)) {
          subscriber.foreach(_.onNext(grab(in)))
          if (requested > 0) {
            pull(in)
            requested -= 1
          }
        }
      }
      val setSubscription = getAsyncCallback[Option[Subscription]] { s =>
        this.subscription = s
        if (isAvailable(out)) subscription.foreach(_.request(1))
      }
      val onProcessorComplete = getAsyncCallback[Unit](_ => complete())
      val onProcessorError = getAsyncCallback[Throwable](fail)

      val pushFromUpstream = getAsyncCallback[R] { r =>
        push(out, r)
      }
      val pullFromUpstream = getAsyncCallback[Long] { n =>
        requested += n
        if (!hasBeenPulled(in)) {
          pull(in)
          requested -= 1
        }
      }

      private def complete(): Unit = {
        subscriber.foreach(_.onComplete())
        subscription.foreach(_.cancel())
        completeStage()
      }
      private def fail(ex: Throwable): Unit = {
        subscriber.foreach(_.onError(ex))
        subscription.foreach(_.cancel())
        failStage(ex)
      }



      setHandlers(in, out, new InHandler with OutHandler {
        //Input
        override def onPush(): Unit = {
          subscriber.foreach(_.onNext(grab(in)))
        }
        override def onUpstreamFinish(): Unit = complete()
        override def onUpstreamFailure(ex: Throwable): Unit = fail(ex)
        //Output
        override def onPull(): Unit = {
          subscription.foreach(_.request(1))
        }
        override def onDownstreamFinish(): Unit = complete()
      })

      override def preStart(): Unit = {
        processor.success(new Processor[R, T] {
          //Publisher
          override def subscribe(s: Subscriber[_ >: T]): Unit = {
            setSubscriber.invoke(Some(s))
            s.onSubscribe(new Subscription {
              override def cancel(): Unit = setSubscriber.invoke(None)
              override def request(n: Long): Unit = pullFromUpstream.invoke(n)
            })
          }
          //Subscriber
          override def onSubscribe(s: Subscription): Unit = setSubscription.invoke(Some(s))
          override def onError(t: Throwable): Unit = onProcessorError.invoke(t)
          override def onComplete(): Unit = onProcessorComplete.invoke()
          override def onNext(r: R): Unit = pushFromUpstream.invoke(r)
        })
      }
    }

    (logic, processor.future)
  }

  override def shape: FlowShape[T, R] = FlowShape(in, out)
}