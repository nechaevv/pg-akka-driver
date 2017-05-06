package com.github.nechaevv.postgresql.marshal

import java.nio.ByteOrder

import slick.collection.heterogeneous.{HCons, HList, HNil}
import slick.collection.heterogeneous.syntax._

/**
  * Created by CONVPN on 5/5/2017.
  */

trait MetaMarshallers {

  implicit val HNilUnmarshaller: Unmarshaller[HNil] = _ => HNil

  implicit def OptionUnmarshaller[T](implicit u: Unmarshaller[T :: HNil]): Unmarshaller[Option[T] :: HNil] = {
    case v@Seq((_, _: Some[_]), _*) => Some(u(v).head) :: HNil
    case _ => None :: HNil
  }

  implicit def HConsUnmarshaller[T1, T2, TT <: HList](implicit u1: Unmarshaller[T1 :: HNil], u2: Unmarshaller[HCons[T2, TT]]): Unmarshaller[T1 :: T2 :: TT] = {
    case Seq(head, tail @ _*) => u1(Seq(head)) ::: u2(tail)
  }

}

trait DefaultMarshallers { this: MetaMarshallers =>

  implicit val StringUnmarshaller: Unmarshaller[String :: HNil] = {
    case Seq((19, Some(bytes)), _*) => bytes.decodeString("UTF-8") :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as String")
  }
  //implicit val StringMarshaller: Marshaller[String] =
  implicit val IntUnmarshaller: Unmarshaller[Int :: HNil] = {
    case Seq((23, Some(bytes)), _*) => bytes.iterator.getInt(ByteOrder.BIG_ENDIAN) :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as Int")
  }

  implicit val BoolUnmarshaller: Unmarshaller[Boolean :: HNil] = {
    case Seq((16, Some(bytes)), _*) => (bytes(0) != 0) :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as Boolean")
  }

}

object DefaultMarshallers extends MetaMarshallers with DefaultMarshallers