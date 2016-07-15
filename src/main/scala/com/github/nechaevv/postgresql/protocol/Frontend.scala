package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.util.ByteIterator

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
object Frontend {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  def decode(p: Packet) = {
    val i = p.payload.iterator
    p.messageType match {
      case 'R' =>
        i.getInt match {
          case 0 => AuthenticationOk
          case 2 => AuthenticationKerberosV5
          case 3 => AuthenticationCleartextPassword
          case 5 => new AuthenticationMD5Password(i, p.length)
          case 6 => AuthenticationSCMCredential
          case 7 => AuthenticationGSS
          case 8 => AuthenticationSSPI
          case 9 => new AuthenticationGSSContinue(i, p.length)
        }
    }
  }


}

case object AuthenticationOk
case object AuthenticationCleartextPassword
case object AuthenticationKerberosV5
case class AuthenticationMD5Password(salt: Array[Byte]) {
  def this(bi: ByteIterator, packetLength: Int) = this(bi.getBytes(packetLength))
}
case object AuthenticationSCMCredential
case object AuthenticationGSS
case object AuthenticationSSPI
case class AuthenticationGSSContinue(authData: Array[Byte]) {
  def this(bi: ByteIterator, packetLength: Int) = this(bi.getBytes(packetLength))
}