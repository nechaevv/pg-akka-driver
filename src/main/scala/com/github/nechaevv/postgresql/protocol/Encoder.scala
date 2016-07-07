package com.github.nechaevv.postgresql.protocol

import akka.util.ByteString

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
trait Encoder[T] {
  def encode(value: T): ByteString
}

object Encoder {
  def apply[T](value: T)(implicit encoder: Encoder[T]): ByteString = encoder.encode(value)
}