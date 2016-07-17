package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.util.{ByteString, ByteStringBuilder}

/**
  * Created by vn on 17.07.16.
  */
package object frontend {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  val protocolVersion = 3

  def encodePacket(t: Byte, msg: ByteString) = ByteString.newBuilder
    .putByte(t).putInt(msg.length + 4).append(msg).result()

  implicit def byteStringBuilderPimp(bsb: ByteStringBuilder): ByteStringBuilderExt = new ByteStringBuilderExt(bsb)

}
