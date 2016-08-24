package com.github.nechaevv.postgresql.api

import akka.stream.scaladsl.Sink
import slick.collection.heterogeneous.HList

/**
  * Created by v.a.nechaev on 19.08.2016.
  */
case class Statement[I <: HList, O <: HList](sql: String)

case class Command[I <: HList, O <: HList](statement: Statement[I, O], parameters: I, sink: Sink[DataRow[O], _])

case class DataRow[T <: HList](value: T)