package com.github.nechaevv.postgresql.marshal

import java.nio.ByteOrder

import com.github.nechaevv.postgresql.connection.{BinaryValue, NullValue, StringValue}
import slick.collection.heterogeneous.{HCons, HList, HNil}
import slick.collection.heterogeneous.syntax._

/**
  * Created by CONVPN on 5/5/2017.
  */

trait MetaMarshallers {

  implicit val HNilUnmarshaller: Unmarshaller[HNil] = _ => HNil

  implicit def OptionUnmarshaller[T](implicit u: Unmarshaller[T :: HNil]): Unmarshaller[Option[T] :: HNil] = {
    case v@Seq((_, NullValue), _*) => None :: HNil
    case v => Some(u(v).head) :: HNil
  }

  implicit def HConsUnmarshaller[T1, T2, TT <: HList](implicit u1: Unmarshaller[T1 :: HNil], u2: Unmarshaller[HCons[T2, TT]]): Unmarshaller[T1 :: T2 :: TT] = {
    case Seq(head, tail @ _*) => u1(Seq(head)) ::: u2(tail)
  }

}

trait DefaultMarshallers { this: MetaMarshallers =>

  implicit val StringUnmarshaller: Unmarshaller[String :: HNil] = {
    case Seq((19, BinaryValue(bytes)), _*) => bytes.decodeString("UTF-8") :: HNil
    case Seq((19, StringValue(s)), _*) => s :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as String")
  }
  //implicit val StringMarshaller: Marshaller[String] =
  implicit val IntUnmarshaller: Unmarshaller[Int :: HNil] = {
    case Seq((23, BinaryValue(bytes)), _*) => bytes.iterator.getInt(ByteOrder.BIG_ENDIAN) :: HNil
    case Seq((23, StringValue(s)), _*) => s.toInt :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as Int")
  }

  implicit val BoolUnmarshaller: Unmarshaller[Boolean :: HNil] = {
    case Seq((16, BinaryValue(bytes)), _*) => (bytes(0) != 0) :: HNil
    case Seq((16, StringValue(s)), _*) => (s == "t") :: HNil
    case u => throw new RuntimeException(s"Cannont decode $u as Boolean")
  }

}

object DefaultMarshallers extends MetaMarshallers with DefaultMarshallers