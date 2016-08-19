package com.github.nechaevv.postgresql.api

import slick.collection.heterogeneous.HList

/**
  * Created by v.a.nechaev on 19.08.2016.
  */
case class Statement[I <: HList, O <: HList](sql: String)

case class Command[I <: HList, O <: HList](statement: Statement[I, O], parameters: I)

case class DataRow[T <: HList](value: T)