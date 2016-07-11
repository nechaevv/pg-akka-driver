package com.github.nechaevv.postgresql

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, FSM}
import akka.actor.Actor.Receive
import akka.io.Tcp.{Connect, Connected}
import akka.io.{IO, Tcp}
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.{Backend, Frontend}

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
class PostgresqlConnection(address: InetSocketAddress, database: String, user: String, password: String) extends FSM[ConnectionState, ConnectionStateData] {

  startWith(Connecting, ConnectionStateData(None, None))

  IO(Tcp) ! Connect(address)

  when(Connecting) {
    case Event(Connected(remote, local), stateData) =>
      self ! Backend.startupMessage(database, user)
      goto(Authorizing) using stateData.copy(serverConnection = Some(sender))
  }

  when(StartingUp) {
    case Event()
    Frontend.decode()
  }

}

object PostgresqlConnection {
    val flow = Flow[ByteString]
  def auhorizeMessage =
}

sealed trait ConnectionState

case object Connecting extends ConnectionState
case object StartingUp extends ConnectionState
case object Authorizing extends ConnectionState
case object Idle extends ConnectionState
case object Conversation extends ConnectionState

case class ConnectionStateData(serverConnection: Option[ActorRef], resultListener: Option[ActorRef])