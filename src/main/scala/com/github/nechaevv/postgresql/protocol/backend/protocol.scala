package com.github.nechaevv.postgresql.protocol.backend
import akka.util.{ByteIterator, ByteString}

import scala.annotation.tailrec

sealed trait BackendMessage

case object AuthenticationOk extends BackendMessage
case object AuthenticationCleartextPassword extends BackendMessage
case object AuthenticationKerberosV5 extends BackendMessage
case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage {
  def this(bi: ByteIterator, packetLength: Int) = this(bi.getBytes(packetLength))
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
    else readErrorFields(bi, (fieldType.toChar, readString(bi)) :: fields)
  }
}

case class UnknownMessage(t: Char, content: ByteString) extends BackendMessage
