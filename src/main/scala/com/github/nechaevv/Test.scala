package com.github.nechaevv

import java.net.InetSocketAddress

import akka.actor.FSM.{SubscribeTransitionCallBack, Transition}
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.github.nechaevv.postgresql.connection.{PostgresqlConnection, QueryCommand, Ready}
import com.github.nechaevv.postgresql.protocol.backend.{CommandComplete, DataRow}
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
object Test extends App with LazyLogging {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()

  logger.info("Connecting")
  as.actorOf(Props[TestActor1])

  def connectionFactory = new PostgresqlConnection(
  InetSocketAddress.createUnresolved("localhost", 5432),
  "trading","trading","trading"
  )
}

class TestActor1 extends Actor with LazyLogging {
  logger.info("Creating connection")
  private val conn = context.actorOf(Props(Test.connectionFactory))
  conn ! SubscribeTransitionCallBack(self)

  var commands = List(QueryCommand("SELECT * FROM \"TEST\"", Nil, 0))

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
