package com.github.nechaevv.postgresql.protocol.backend
import akka.util.{ByteIterator, ByteString}

import scala.annotation.tailrec

sealed trait BackendMessage

case object AuthenticationOk extends BackendMessage
case object AuthenticationCleartextPassword extends BackendMessage
case object AuthenticationKerberosV5 extends BackendMessage
case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage {
  def this(bi: ByteIterator) = this(bi.getBytes(4))
}
case object AuthenticationSCMCredential extends BackendMessage
case object AuthenticationGSS extends BackendMessage
case object AuthenticationSSPI extends BackendMessage
case class AuthenticationGSSContinue(authData: Array[Byte]) extends BackendMessage {
  def this(bi: ByteIterator, packetLength: Int) = this(bi.getBytes(packetLength))
}
case class ErrorMessage(errorFields: Seq[(Char, String)]) extends BackendMessage {
  def this(bi: ByteIterator) = this(ErrorMessage.readErrorFields(bi, Nil))
}

object ErrorMessage {
  @tailrec
  def readErrorFields(bi: ByteIterator, fields: List[(Char, String)]): List[(Char, String)] = {
    val fieldType = bi.getByte
    if (fieldType == 0) fields
    else readErrorFields(bi, (fieldType.toChar, bi.getNullTerminatedString) :: fields)
  }
}

case class CommandComplete(tag: String) extends BackendMessage {
  def this(bi: ByteIterator) = this(bi.getNullTerminatedString)
}

case class FieldDescription(name: String, tableId: Int, attributeNumber: Short, dataTypeOid: Int, size: Short, typeModifier: Int, formatCode: Short) {
  def this(bi: ByteIterator) = this(bi.getNullTerminatedString, bi.getInt, bi.getShort, bi.getInt, bi.getShort, bi.getInt, bi.getShort)
}
case class RowDescription(fields: Seq[FieldDescription]) extends BackendMessage {
  def this(bi: ByteIterator) = this(for(i <- 1 to bi.getShort) yield new FieldDescription(bi))
}

case class DataRow(values: Seq[Option[ByteString]]) extends BackendMessage {
  def this(bi: ByteIterator) = this(for(i <- 1 to bi.getShort) yield bi.getInt match {
    case -1 => None
    case n => Some(bi.getByteString(n))
  })
}

case class ReadyForQuery(txStatus: Char) extends BackendMessage {
  def this(bi: ByteIterator) = this(bi.getByte.toChar)
}

case object EmptyQueryResponse extends BackendMessage

case class UnknownMessage(t: Char, content: ByteString) extends BackendMessage
