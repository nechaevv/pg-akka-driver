package com.github.nechaevv.postgresql.api

import akka.util.ByteString

/**
  * Created by v.a.nechaev on 19.08.2016.
  */
trait PgTypeMapper[T] {
  def read(oid: Int, bytes: ByteString): T
  def write(value: T): ByteString
  def oid: Int
}
