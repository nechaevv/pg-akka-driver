package com.github.nechaevv.postgresql.connection

import akka.util.ByteString

sealed trait SqlCommand

case class Query(sql: String, parameterTypes: Seq[Int])

case class Statement(query: Query, parameters: Seq[PgValue], resultFormats: Seq[Int]) extends SqlCommand

case class SimpleQuery(sql: String) extends SqlCommand

sealed trait CommandResult

case class ResultRow(data: Seq[(Int, PgValue)]) extends CommandResult

case object CommandCompleted extends CommandResult

case class CommandFailed(code: String, message: String, detail: Option[String]) extends CommandResult

sealed trait PgValue

case class StringValue(value: String) extends PgValue
case class BinaryValue(value: ByteString) extends PgValue
case object NullValue extends PgValue

case class ValueSpec(oid: Int, format: Int)

object ValueFormats {
  val Text = 0
  val Binary = 1
}
