package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.util.ByteString

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
object Frontend {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  def decode(bs: ByteString) = {
    val i = bs.iterator
    i.getByte match {
      case 'R' =>
        val l = i.getInt
        i.getInt match {
          case 0 => AuthenticationOk
          case 2 => AuthenticationKerberosV5
          case 3 => AuthenticationCleartextPassword
          case 5 => AuthenticationMD5Password(i.getBytes(4))
          case 6 => AuthenticationSCMCredential
          case 7 => AuthenticationGSS
          case 8 => AuthenticationSSPI
          case 9 => AuthenticationGSSContinue(i.getBytes(l - 8))
        }
    }
  }


}

case object AuthenticationOk
case object AuthenticationCleartextPassword
case object AuthenticationKerberosV5
case class AuthenticationMD5Password(salt: Array[Byte])
case object AuthenticationSCMCredential
case object AuthenticationGSS
case object AuthenticationSSPI
case class AuthenticationGSSContinue(authData: Array[Byte])