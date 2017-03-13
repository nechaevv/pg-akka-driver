package com.github.nechaevv.postgresql.connection

import akka.util.ByteString

case class SqlCommand(sql: String, params: Seq[(Int, Option[ByteString])])

sealed trait CommandResult

case class ResultRow(data: Seq[(Int, Option[ByteString])]) extends CommandResult

case object CommandCompleted extends CommandResult