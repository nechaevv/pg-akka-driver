package com.github.nechaevv.postgresql.protocol

import akka.util.ByteString

/**
  * Created by v.a.nechaev on 11.07.2016.
  */
object Backend {

  val protocolVersion = 3

  def encodePacket(t: Byte, msg: ByteString) = ByteString.newBuilder
    .putByte(t).putInt(msg.length + 4).append(msg).result()

  def startupMessage(database: String, user: String) = {
    val bs = ByteString.newBuilder
      .putInt(protocolVersion << 16)
      .putBytes("database".getBytes).putBytes(database.getBytes)
      .putBytes("user".getBytes).putBytes(user.getBytes).result()
    ByteString.newBuilder.putInt(bs.length + 4).append(bs).result()
  }

}
