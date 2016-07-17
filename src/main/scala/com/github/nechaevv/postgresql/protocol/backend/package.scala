package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.util.{ByteIterator, ByteString}

import scala.annotation.tailrec

/**
  * Created by vn on 17.07.16.
  */
package object backend {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN

  def readString(bi: ByteIterator): String = {
    val bb = ByteString.newBuilder
    @tailrec
    def readNextSymbol(): Unit = {
      val c = bi.getByte
      if (c > 0) {
        bb.putByte(c)
        readNextSymbol()
      }
    }
    readNextSymbol()
    bb.result().decodeString("UTF-8")
  }

}
