package com.github.nechaevv.postgresql.connection
import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}

/**
  * Created by CONVPN on 5/5/2017.
  */
class ConnectionPoolImpl extends ConnectionPool {

  override def run(cmd: SqlCommand): Source[CommandResult, NotUsed] = ???

  override def acquireConnecion: Flow[SqlCommand, CommandResult, NotUsed] = ???

}
