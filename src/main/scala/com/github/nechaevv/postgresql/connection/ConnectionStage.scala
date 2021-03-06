package com.github.nechaevv.postgresql.connection

import java.security.MessageDigest
import java.util.UUID

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.backend
import com.github.nechaevv.postgresql.protocol.frontend
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by vn on 11.03.17.
  */
class ConnectionStage(database: String, username: String, password: String)
  extends GraphStage[BidiShape[SqlCommand, frontend.FrontendMessage, backend.BackendMessage, CommandResult]]
  with LazyLogging {

  val commandIn = Inlet[SqlCommand]("ConnectionStage.command.in")
  val resultOut = Outlet[CommandResult]("ConnectionStage.command.Out")
  val pgIn = Inlet[backend.BackendMessage]("ConnectionStage.db.in")
  val pgOut = Outlet[frontend.FrontendMessage]("ConnectionStage.db.out")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var state: ConnectionStageState = Initializing
    var preparedStatements: Map[Query, (UUID, Seq[Int], Seq[Int])] = Map.empty

    private def handleEvent(): Unit = {
      //logger.trace(s"Event: state $state commandIn: ${isAvailable(commandIn)}, pgOut: ${isAvailable(pgOut)}, " +
      //  s"pgIn: ${isAvailable(pgIn)}, cmdOut: ${isAvailable(resultOut)}")
      state match {
        case Initializing => if (isAvailable(pgOut)) {
          logger.trace("Connecting")
          push(pgOut, frontend.StartupMessage(database, username))
          pull(pgIn)
          state = Connecting
        }
        case Connecting => if (isAvailable(pgIn) && isAvailable(pgOut)) {
          val cmd = grab(pgIn)
          cmd match {
            case backend.AuthenticationCleartextPassword =>
              logger.trace("Requested cleartext auth")
              push(pgOut, frontend.PasswordMessage(password))
              pull(pgIn)
            case backend.AuthenticationMD5Password(salt) =>
              logger.trace("Requested md5 auth")
              push(pgOut, frontend.PasswordMessage(md5password(username, password, salt)))
              pull(pgIn)
            case backend.AuthenticationOk =>
              logger.trace("Authentication succeeded")
              pull(pgIn)
            case backend.ParameterStatus(name, value) =>
              logger.trace(s"Parameter $name=$value")
              pull(pgIn)
            case backend.BackendKeyData(pid, key) =>
              logger.trace(s"Backend key data: pid $pid, key: $key")
              pull(pgIn)
            case msg: backend.ReadyForQuery => readyForQuery(msg)
            case msg: backend.ErrorMessage => handleError(msg)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }
        case ReadyForCommand => if (isAvailable(pgIn)) { //&& isAvailable(resultOut)) {
          grab(pgIn) match {
            case err: backend.ErrorMessage => handleError(err)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        } else if (isAvailable(commandIn) && isAvailable(pgOut)) {
          grab(commandIn) match {
            case cmd: Statement =>
              runPreparedStatement(cmd)
            case SimpleQuery(sql) =>
              logger.trace(s"Executing simple query $sql")
              push(pgOut, frontend.Query(sql))
              state = ExecutingSimpleQuery
            case msg =>
              logUnknownMessage(msg)
              pull(commandIn)
          }
        }
        case ExecutingSimpleQuery => if (isAvailable(pgIn)) {
          grab(pgIn) match {
            case backend.RowDescription(fields) =>
              logger.trace(s"Result columns: ${(for (field <- fields) yield s"${field.name}(${field.dataTypeOid})").mkString(",")}")
              val columnTypes = fields.map(_.dataTypeOid)
              pull(pgIn)
              state = Executing(columnTypes, Nil)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }

        case currentState@Parsing(psId, cmd, _, _) => if (isAvailable(pgIn) && isAvailable(pgOut)) {
          def storeStatementAndExecuteIfCompleted(s: Parsing): Unit = {
            s match {
              case Parsing(_, _, Some(ct), Some(pt)) =>
                preparedStatements += cmd.query -> (psId, ct, pt)
                state = Executing(ct, cmd.resultFormats)
                //doBind(psId, cmd, ct, pt)
                pull(pgIn)
              case _ =>
                state = s
                pull(pgIn)
            }
          }
          grab(pgIn) match {
            case backend.RowDescription(fields) =>
              logger.trace(s"Result columns: ${(for (field <- fields) yield s"${field.name}(${field.dataTypeOid})").mkString(",")}")
              storeStatementAndExecuteIfCompleted(currentState.copy(columnTypes = Some(fields.map(_.dataTypeOid))))
            case backend.ParameterDescription(paramTypes) =>
              logger.trace(s"Parameter types: ${paramTypes.mkString(",")}")
              storeStatementAndExecuteIfCompleted(currentState.copy(paramTypes = Some(paramTypes)))
            case backend.ParseComplete =>
              logger.trace(s"Parse complete")
              pull(pgIn)
            case msg: backend.ReadyForQuery => readyForQuery(msg)
            case msg: backend.ErrorMessage => handleError(msg)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }
        }
        case Executing(columnTypes, resultFormats) => if (isAvailable(pgIn) && isAvailable(resultOut)) {
          grab(pgIn) match {
            case backend.BindComplete =>
              logger.trace("Bind completed, executing query")
              pull(pgIn)
            case backend.DataRow(row) =>
              logger.trace("Data row received")
              push(resultOut, ResultRow(columnTypes zip row.zip(resultFormats.padTo(row.length, ValueFormats.Text)).map({
                case (Some(bs), ValueFormats.Text) => StringValue(bs.decodeString("UTF-8"))
                case (Some(bs), ValueFormats.Binary) => BinaryValue(bs)
                case (Some(_), _) => throw new IllegalArgumentException
                case (None, _) => NullValue
              })))
              pull(pgIn)
            case backend.CommandComplete(_) =>
              logger.trace("SQL command completed")
              push(resultOut, CommandCompleted)
              pull(pgIn)
              //pullCommand()
              //state = ReadyForCommand
            case msg: backend.ReadyForQuery => readyForQuery(msg)
            case msg: backend.ErrorMessage => handleError(msg)
            case msg =>
              logUnknownMessage(msg)
              pull(pgIn)
          }

        }
        case Queued(msg :: rest, nextState) => if (isAvailable(pgOut)) {
          push(pgOut, msg)
          if (rest.isEmpty) {
            state = nextState
          } else state = Queued(rest, nextState)
          logger.trace(s"Sending queued message $msg, next state $state")
        }
        case Queued(Nil, _) => //impossible state
      }
    }

    def readyForQuery(msg: backend.ReadyForQuery): Unit = {
      logger.info(s"Ready for query (tx status ${msg.txStatus})")
      state = ReadyForCommand
      pullCommand()
      pull(pgIn)
    }

    def handleError(errorMessage: backend.ErrorMessage): Unit = {
      val msg = errorMessage.errorFields.foldLeft(CommandFailed("","", None))((r, field) =>
        field._1 match {
          case 'C' => r.copy(code = field._2)
          case 'M' => r.copy(message = field._2)
          case 'D' => r.copy(detail = Some(field._2))
          case _ => r
        })
      logger.error(msg.toString)
      push(resultOut, msg)
      pull(pgIn)
    }

    def pgValueToBS(v: PgValue) = v match {
      case BinaryValue(v) => (Some(v), ValueFormats.Binary)
      case StringValue(v) => (Some(ByteString(v)), ValueFormats.Text)
      case NullValue => (None, ValueFormats.Text)
    }

    def runPreparedStatement(cmd: Statement): Unit = {
      def bindAndExecute(psId: String) = {
        val (paramValues, paramFormats) = cmd.parameters.map(pgValueToBS).unzip
        List(
          frontend.Bind("", psId, paramFormats, paramValues, cmd.resultFormats),
          frontend.Execute("", 0),
          frontend.Sync
        )
      }
      val (psId, firstCommand :: nextCommands, nextState) = preparedStatements.get(cmd.query) match {
        case Some((psId, columnTypes, paramTypes)) =>  (psId, bindAndExecute(psId.toString), Executing(columnTypes, cmd.resultFormats))
        case None =>
          val psId = UUID.randomUUID()
          logger.trace(s"Preparing query ${cmd.query.sql} with name $psId")
          (psId, frontend.Parse(psId.toString, cmd.query.sql, cmd.query.parameterTypes) :: frontend.DescribeStatement(psId.toString) :: bindAndExecute(psId.toString),
            Parsing(psId, cmd, None, None))
      }
      push(pgOut, firstCommand)
      state = Queued(nextCommands, nextState)
    }
/*
    def doBind(psId: UUID, cmd: Statement, columnTypes: Seq[Int], paramTypes: Seq[Int]): Unit = {
      //logger.trace(s"Binding prepared statement $psId with ${cmd.params.length} parameters")
      //if (!cmd.params.map(_._1).sameElements(paramTypes)) throw new RuntimeException("Wrong parameter types")
      push(pgOut, Bind("", psId.toString, Nil, cmd.params.map(_._2), Seq.fill(columnTypes.length)(1)))
      state = Queued(List(Execute("", 0), Sync), Executing(columnTypes))
    }
*/
    def pullCommand(): Unit = {
      if (isClosed(commandIn)) {
        disconnect()
        completeStage()
      } else pull(commandIn)
    }

    def logUnknownMessage(msg: Any) = logger.error(s"Unexpected message $msg for state $state")

    setHandler(commandIn, new InHandler {
      override def onPush(): Unit = {
        //logger.trace("cmd.in push")
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
        //logger.trace("pg.out pull")
        handleEvent()
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })
    setHandler(pgIn, new InHandler {
      override def onPush(): Unit = {
        //logger.trace("pg.in push")
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
        //logger.trace("result.out pull")
        handleEvent()
      }
      override def onDownstreamFinish(): Unit = completeStage()
    })

    def disconnect(): Unit = {
      logger.trace("Terminating connection")
      if (isAvailable(pgOut)) push(pgOut, frontend.Terminate)
    }

  }

  override def shape: BidiShape[SqlCommand, frontend.FrontendMessage, backend.BackendMessage, CommandResult] = {
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
case class Executing(columnTypes: Seq[Int], resultFormats: Seq[Int]) extends ConnectionStageState
case class Queued(msgs: List[frontend.FrontendMessage], state: ConnectionStageState) extends ConnectionStageState
