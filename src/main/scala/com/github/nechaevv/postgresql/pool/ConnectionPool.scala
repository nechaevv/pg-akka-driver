package com.github.nechaevv.postgresql.pool

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.nechaevv.postgresql.connection.{ResultRow, SqlCommand}
import com.github.nechaevv.postgresql.marshal.Unmarshaller
import slick.collection.heterogeneous.HList

import scala.concurrent.Future

/**
  * Created by CONVPN on 5/5/2017.
  */
trait ConnectionPool {
  def run[T <: HList](cmd: SqlCommand)(implicit um: Unmarshaller[T]): Source[T, NotUsed]
  def terminate(): Future[Unit]
}
