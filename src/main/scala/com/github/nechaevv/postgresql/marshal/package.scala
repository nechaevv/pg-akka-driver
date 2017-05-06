package com.github.nechaevv.postgresql

import akka.util.ByteString
import _root_.slick.collection.heterogeneous.HList

/**
  * Created by CONVPN on 5/5/2017.
  */
package object marshal {
  type Unmarshaller[T <: HList] = Seq[(Int, Option[ByteString])] => T
  type Marshaller[T] = T => Option[ByteString]
}
