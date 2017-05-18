package com.github.nechaevv.postgresql

import _root_.slick.collection.heterogeneous.HList
import com.github.nechaevv.postgresql.connection.PgValue

/**
  * Created by CONVPN on 5/5/2017.
  */
package object marshal {
  type Unmarshaller[T <: HList] = Seq[(Int, PgValue)] => T
  type Marshaller[T] = T => PgValue
}
