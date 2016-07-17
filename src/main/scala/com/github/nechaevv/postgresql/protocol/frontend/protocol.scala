package com.github.nechaevv.postgresql.protocol.frontend

import akka.util.ByteString

/**
  * Created by vn on 17.07.16.
  */
sealed trait FrontendMessage {
  def encode: ByteString
}

case class StartupMessage(database: String, user: String) extends FrontendMessage {
  override def encode: ByteString = {
    val bs = ByteString.newBuilder
      .putInt(protocolVersion << 16)
      .putNullTerminatedString("database").putNullTerminatedString(database)
      .putNullTerminatedString("user").putNullTerminatedString(user)
      .putByte(0).result()
    ByteString.newBuilder.putInt(bs.length + 4).append(bs).result()
  }
}

case class PasswordMessage(password: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket('p', ByteString.newBuilder.putNullTerminatedString(password).result())
}
