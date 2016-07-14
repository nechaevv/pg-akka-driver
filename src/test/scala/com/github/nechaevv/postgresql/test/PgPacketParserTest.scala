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

  "Parser" should "correctly parse single ByteString input" in {
    val source = Source.fromIterator(() => Seq(testInput1).iterator)
    val sink = Sink.seq[Packet]
    val parserStage = new PgPacketParser
    val result = Await.result(source.via(parserStage).runWith(sink), 1.minute)
    result should have length 1
    result(1) shouldBe Packet(0x01, 6, ByteString(0x01, 0x02))
  }

}

object PgPacketParserTest {
  val testInput1 = ByteString(0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x02)
}