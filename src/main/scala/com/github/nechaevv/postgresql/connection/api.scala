package com.github.nechaevv.postgresql.connection

import akka.util.ByteString

sealed trait SqlCommand

case class Statement(sql: String, columnCount: Int, params: Seq[(Int, Option[ByteString])]) extends SqlCommand

case class SimpleQuery(sql: String) extends SqlCommand

sealed trait CommandResult

case class ResultRow(data: Seq[(Int, Option[ByteString])]) extends CommandResult

case object CommandCompleted extends CommandResult

case class CommandFailed(code: String, message: String, detail: Option[String]) extends CommandResult