package com.github.nechaevv.postgresql.connection

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}

/**
  * Created by CONVPN on 5/5/2017.
  */
trait ConnectionPool {
  def run(cmd: SqlCommand): Source[CommandResult, NotUsed]
  def acquireConnecion: Flow[SqlCommand, CommandResult, NotUsed]
}
