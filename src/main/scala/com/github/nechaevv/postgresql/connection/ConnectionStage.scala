package com.github.nechaevv.postgresql.connection

import java.security.MessageDigest
import java.util.UUID

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.github.nechaevv.postgresql.protocol.backend.{AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationOk, BackendKeyData, BackendMessage, BindComplete, CommandComplete, DataRow, ParameterDescription, ParameterStatus, ParseComplete, ReadyForQuery, RowDescription}
import com.github.nechaevv.postgresql.protocol.frontend.{Bind, DescribeStatement, Execute, FrontendMessage, Parse, PasswordMessage, Query, StartupMessage, Sync, Terminate}
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by vn on 11.03.17.
  */
class ConnectionStage(database: String, username: String, password: String)
  extends GraphStage[BidiShape[SqlCommand, FrontendMessage, BackendMessage, CommandResult]]
  with LazyLogging {

  val commandIn = Inlet[SqlCommand]("ConnectionStage.api.in")
  val resultOut = Outlet[CommandResult]("Connection.command.Out")
  val pgIn = Inlet[BackendMessage]("ConnectionStage.db.in")
  val pgOut = Outlet[FrontendMessage]("ConnectionStage.db.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var state: ConnectionStageState = Initializing
    var preparedStatements: Map[String, (UUID, Seq[Int], Seq[Int])] = Map.empty

    private def handleEvent(): Unit = {
      //logger.trace(s"Event: state $state commandIn: ${isAvailable(commandIn)}, pgOut: ${isAvailable(pgOut)}, " +
      //  s"pgIn: ${isAvailable(pgIn)}, cmdOut: ${isAvailable(resultOut)}")
      state match {
        case Initializing => if (isAvailable(pgOut)) {
          logger.trace("Connecting")
          push(pgOut, StartupMessage(database, username))
          pull(pgIn)
          state = Connecting
        }
        case Connecting => if (isAvailable(pgIn) && isAvailable(pgOut)) {
          val cmd = grab(pgIn)
          cmd match {
            case AuthenticationCleartextPassword =>
              logger.trace("Requested cleartext auth")
              push(pgOut, PasswordMessage(password))
              pull(pgIn)
            case AuthenticationMD5Password(salt) =>
              logger.trace("Requested md5 auth")
              push(pgOut, PasswordMessage(md5password(username, password, salt)))
              pull(pgIn)
            case AuthenticationOk =>
              logger.trace("Authentication succeeded")
              pull(pgIn)
            case ParameterStatus(name, value) =>
              logger.trace(s"Parameter $name=$value")
              pull(pgIn)
            case BackendKeyData(pid, key) =>
              logger.trace(s"Backend key data: pid $pid, key: $key")
              pull(pgIn)
            case ReadyForQuery(txStatus) =>
              logger.info(s"Connection ready (tx $txStatus)")
              state = ReadyForCommand
              pullCommand()
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }
        case ReadyForCommand => if (isAvailable(commandIn) && isAvailable(pgOut)) {
          grab(commandIn) match {
            case cmd: Statement =>
              preparedStatements.get(cmd.sql) match {
                case Some((psId, columnTypes, paramTypes)) => doBind(psId, cmd, columnTypes, paramTypes)
                case None =>
                  val psId = UUID.randomUUID()
                  logger.trace(s"Preparing query ${cmd.sql} with name $psId")
                  push(pgOut, Parse(psId.toString, cmd.sql, Nil))
                  state = Queued(List(DescribeStatement(psId.toString), Sync), Parsing(psId, cmd, None, None))
              }
            case SimpleQuery(sql) =>
              logger.trace(s"Executing simple query $sql")
              push(pgOut, Query(sql))
              pull(pgIn)
              state = ExecutingSimpleQuery
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }
        case ExecutingSimpleQuery => if (isAvailable(pgIn)) {
          grab(pgIn) match {
            case RowDescription(fields) =>
              logger.trace(s"Result columns: ${(for (field <- fields) yield s"${field.name}(${field.dataTypeOid})").mkString(",")}")
              val columnTypes = fields.map(_.dataTypeOid)
              pull(pgIn)
              state = Executing(columnTypes)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }

        case currentState@Parsing(psId, cmd, _, _) => if (isAvailable(pgIn) && isAvailable(pgOut)) {
          def storeStatementAndExecuteIfCompleted(s: Parsing): Unit = {
            s match {
              case Parsing(_, _, Some(ct), Some(pt)) =>
                preparedStatements += cmd.sql -> (psId, ct, pt)
                doBind(psId, cmd, ct, pt)
              case _ =>
                state = s
                pull(pgIn)
            }
          }
          grab(pgIn) match {
            case RowDescription(fields) =>
              logger.trace(s"Result columns: ${(for (field <- fields) yield s"${field.name}(${field.dataTypeOid})").mkString(",")}")
              storeStatementAndExecuteIfCompleted(currentState.copy(columnTypes = Some(fields.map(_.dataTypeOid))))
            case ParameterDescription(paramTypes) =>
              logger.trace(s"Parameter types: ${paramTypes.mkString(",")}")
              storeStatementAndExecuteIfCompleted(currentState.copy(paramTypes = Some(paramTypes)))
            case ParseComplete =>
              logger.trace(s"Parse complete")
              pull(pgIn)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }
        case Executing(columnTypes) => if (isAvailable(pgIn) && isAvailable(resultOut)) {
          grab(pgIn) match {
            case BindComplete =>
              logger.trace("Bind completed, executing query")
              pull(pgIn)
            case DataRow(row) =>
              logger.trace("Data row received")
              push(resultOut, ResultRow(columnTypes zip row))
              pull(pgIn)
            case CommandComplete(_) =>
              logger.trace("SQL command completed")
              push(resultOut, CommandCompleted)
              pullCommand()
              state = ReadyForCommand
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }

        }
        case Queued(msg :: rest, nextState) => if (isAvailable(pgOut)) {
          push(pgOut, msg)
          if (rest.isEmpty) {
            pull(pgIn)
            state = nextState
          } else state = Queued(rest, nextState)
          logger.trace(s"Sending queued message $msg, next state $state")
        }
        case Queued(Nil, _) => //impossible state
      }
    }

    def doBind(psId: UUID, cmd: Statement, columnTypes: Seq[Int], paramTypes: Seq[Int]): Unit = {
      logger.trace(s"Binding prepared statement $psId with ${cmd.params.length} parameters")
      if (!cmd.params.map(_._1).sameElements(paramTypes)) throw new RuntimeException("Wrong parameter types")
      push(pgOut, Bind("", psId.toString, Nil, cmd.params.map(_._2), Seq.fill(columnTypes.length)(1)))
      pull(pgIn)
      state = Queued(List(Execute("", 0), Sync), Executing(columnTypes))
    }

    def pullCommand(): Unit = {
      if (isClosed(commandIn)) {
        disconnect()
        completeStage()
      } else pull(commandIn)
    }

    def logUnknownMessage(msg: Any) = logger.error(s"Unexpected message $msg for state $state")

    setHandler(commandIn, new InHandler {
      override def onPush(): Unit = {
        handleEvent()
      }
      override def onUpstreamFinish(): Unit = {
        if (state == ReadyForCommand) {
          disconnect()
          completeStage()
        }
      }
      override def onUpstreamFailure(ex: Throwable): Unit = {
        disconnect()
        failStage(ex)
      }
    })

    setHandler(pgOut, new OutHandler {
      override def onPull(): Unit = {
        handleEvent()
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })
    setHandler(pgIn, new InHandler {
      override def onPush(): Unit = {
        handleEvent()
      }
      override def onUpstreamFinish(): Unit = {
        disconnect()
        completeStage()
      }
      override def onUpstreamFailure(ex: Throwable): Unit = {
        disconnect()
        failStage(ex)
      }
    })
    setHandler(resultOut, new OutHandler {
      override def onPull(): Unit = {
        handleEvent()
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })

    def disconnect(): Unit = {
      logger.trace("Terminating connection")
      if (isAvailable(pgOut)) push(pgOut, Terminate)
    }

  }

  override def shape: BidiShape[SqlCommand, FrontendMessage, BackendMessage, CommandResult] = {
    BidiShape(commandIn, pgOut, pgIn, resultOut)
  }

  private def md5password(user: String, password: String, salt: Array[Byte]): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(password.getBytes())
    md.update(user.getBytes())
    md.update(toHex(md.digest()).getBytes())
    md.update(salt)
    "md5" + toHex(md.digest())
  }
  private def toHex(bytes: Array[Byte]): String = bytes.map(b => "%02x".format(b & 0xFF)).mkString

}

sealed trait ConnectionStageState

case object Initializing extends ConnectionStageState
case object Connecting extends ConnectionStageState
case object ReadyForCommand extends ConnectionStageState
case object ExecutingSimpleQuery extends ConnectionStageState
case class Parsing(stmtId: UUID, cmd: Statement, columnTypes: Option[Seq[Int]], paramTypes: Option[Seq[Int]]) extends ConnectionStageState
case class Executing(columnTypes: Seq[Int]) extends ConnectionStageState
case class Queued(msgs: List[FrontendMessage], state: ConnectionStageState) extends ConnectionStageState
