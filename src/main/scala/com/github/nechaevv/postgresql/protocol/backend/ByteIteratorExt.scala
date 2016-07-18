package com.github.nechaevv.postgresql.protocol.backend

import akka.util.{ByteIterator, ByteString}

import scala.annotation.tailrec

/**
  * Created by v.a.nechaev on 18.07.2016.
  */
class ByteIteratorExt(bi: ByteIterator) {
  def getNullTerminatedString = {
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
