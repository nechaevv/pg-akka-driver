package com.github.nechaevv.postgresql.protocol.frontend

import akka.util.ByteStringBuilder

/**
  * Created by vn on 17.07.16.
  */
class ByteStringBuilderExt(builder: ByteStringBuilder) {
  def putNullTerminatedString(s: String) = builder.putBytes(s.getBytes()).putByte(0)
}
