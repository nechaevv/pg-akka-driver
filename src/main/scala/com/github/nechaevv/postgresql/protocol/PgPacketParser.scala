package com.github.nechaevv.postgresql.protocol

import java.nio.ByteOrder

import akka.stream.scaladsl.Framing.FramingException
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString

import scala.annotation.tailrec

/**
  * Created by v.a.nechaev on 14.07.2016.
  */
class PgPacketParser extends GraphStage[FlowShape[ByteString, Packet]] {
  val in = Inlet[ByteString]("PgPackerParserStage.in")
  val out = Outlet[Packet]("PgPackerParserStage.out")

  override val shape: FlowShape[ByteString, Packet] = FlowShape(in, out)
  override def toString: String = "DelimiterFraming"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    implicit val byteOrder = ByteOrder.BIG_ENDIAN
    val headerLength = 4

    var currentPacket: Option[Packet] = None
    var buffer: ByteString = ByteString.empty

    override def onPush(): Unit = {
      buffer ++= grab(in)
      doParse()
      if (isAvailable(in)) packetReady foreach { packet =>
        push(out, packet)
        currentPacket = None
        doParse()
      }
    }

    override def onPull(): Unit = packetReady match {
      case Some(packet) =>
        push(out, packet)
        currentPacket = None
        doParse()
      case None => tryPull()
    }

    private def packetReady() = currentPacket collect {
      case packet if packet.length == packet.payload.length + headerLength => packet
    }

    override def onUpstreamFinish(): Unit = {
      if (isAvailable(in)) packetReady.foreach { packet =>
        push(out, packet)
        currentPacket = None
        doParse()
      }
      if (currentPacket.isEmpty) completeStage()
    }

    private def doParse(): Unit = currentPacket match {
      case Some(packet) =>
        val (payload, leftover) = buffer splitAt (packet.payload.length - packet.length - headerLength)
        currentPacket = Some(packet.copy(payload = packet.payload ++ payload))
        buffer = leftover
      case None => if (buffer.length >= headerLength + 1) {
        val iter = buffer.iterator
        val packetType = iter.getByte
        val packetLength = iter.getInt
        val payloadLength = Math.min(packetLength - headerLength, buffer.length - headerLength - 1)
        val payload = iter.getByteString(payloadLength)
        val leftoverLength = buffer.length - payloadLength - headerLength - 1
        val leftover = if (leftoverLength > 0) iter.getByteString(leftoverLength) else ByteString.empty
        currentPacket = Some(Packet(packetType, packetLength, payload))
        buffer = leftover
      }
    }

    private def tryPull() = {
      if (isClosed(in)) {
        failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))
      } else pull(in)
    }

    setHandlers(in, out, this)

  }

}
