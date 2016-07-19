package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.util.{ByteIterator, ByteString}

import scala.annotation.tailrec

/**
  * Created by vn on 17.07.16.
  */
package object backend {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  implicit def byteIteratorPimp(bi: ByteIterator): ByteIteratorExt = new ByteIteratorExt(bi)

  @tailrec
  def readFieldMap(bi: ByteIterator, fields: List[(Char, String)]): List[(Char, String)] = {
    val fieldType = bi.getByte
    if (fieldType == 0) fields
    else readFieldMap(bi, (fieldType.toChar, bi.getNullTerminatedString) :: fields)
  }

}
