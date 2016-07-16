package com.github.nechaevv.postgresql.test

import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.github.nechaevv.postgresql.protocol.{Packet, PgPacketParser}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by v.a.nechaev on 14.07.2016.
  */
class PgPacketParserTest extends FlatSpec with Matchers {
  import PgPacketParserTest._
  import TestSuite._

  val sink = Sink.seq[Packet]
  val parserStage = new PgPacketParser

  "Parser" should "parse single ByteString input" in {
    val source = Source(List(testInput1))
    val result = Await.result(source.via(parserStage).runWith(sink), 5.seconds)
    result should have length 1
    result.head shouldBe Packet(0x01, ByteString(0x01, 0x02))
  }
  it should "parse message sequence" in {
    val source = Source(List(testInput1, testInput2))
    val result = Await.result(source.via(parserStage).runWith(sink), 5.seconds)
    result should have length 2
    result.head shouldBe Packet(1, ByteString(1,2))
    result.last shouldBe Packet(3, ByteString(1,2,3,4))
  }

  it should "parse several messages from single bytestring" in {
    val source = Source(List(testInput1 ++ testInput2))
    val result = Await.result(source.via(parserStage).runWith(sink), 5.seconds)
    result should have length 2
    result.head shouldBe Packet(1, ByteString(1,2))
    result.last shouldBe Packet(3, ByteString(1,2,3,4))
  }

  it should "parse messages spit to random chunks" in {
    val source = Source(List(testInput1.take(3), testInput1.drop(3) ++ testInput2.take(5), testInput2.drop(5)))
    val result = Await.result(source.via(parserStage).runWith(sink), 5.seconds)
    result should have length 2
    result.head shouldBe Packet(1, ByteString(1,2))
    result.last shouldBe Packet(3, ByteString(1,2,3,4))
  }


}

object PgPacketParserTest {
  val testInput1 = ByteString(0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x02)
  val testInput2 = ByteString(0x03, 0x00, 0x00, 0x00, 0x08, 0x01, 0x02, 0x03, 0x04)
}