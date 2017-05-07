package com.github.nechaevv.postgresql.pool

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.nechaevv.postgresql.connection.{ResultRow, SqlCommand}

import scala.concurrent.Future

/**
  * Created by CONVPN on 5/5/2017.
  */
trait ConnectionPool {
  def run(cmd: SqlCommand): Source[ResultRow, NotUsed]
  def terminate(): Future[Unit]
}
