package com.github.nechaevv.postgresql

import java.nio.ByteOrder

/**
  * Created by sgl on 15.07.16.
  */
package object protocol {
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
}
