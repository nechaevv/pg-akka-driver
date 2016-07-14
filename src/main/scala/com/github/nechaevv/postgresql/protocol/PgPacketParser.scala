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
      pushIfReady()
    }

    override def onPull(): Unit = {
      pushIfReady()
      tryPull()
    }

    private def pushIfReady(): Unit = currentPacket foreach { packet =>
      if (isAvailable(out) && packet.length == packet.payload.length + headerLength) {
        push(out, packet)
        currentPacket = None
      }
    }

    override def onUpstreamFinish(): Unit = {
      pushIfReady()
      completeStage()
    }

    private def doParse(): Unit = {
      if (buffer.length >= headerLength + 1) {
        val (newPacket, leftover) = currentPacket match {
          case Some(packet) =>
            val (payload, leftover) = buffer splitAt (packet.payload.length - packet.length - headerLength)
            (packet.copy(payload = packet.payload ++ payload), leftover)
          case None =>
            val iter = buffer.iterator
            val packetType = iter.getByte
            val packetLength = iter.getInt
            val payloadLength = Math.min(packetLength - headerLength, buffer.length - headerLength - 1)
            val payload = iter.getByteString(payloadLength)
            val leftoverLength = buffer.length - payloadLength - headerLength - 1
            val leftover = if (leftoverLength > 0) iter.getByteString(leftoverLength) else ByteString.empty
            (Packet(packetType, packetLength, payload), leftover)
        }
        currentPacket = Some(newPacket)
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
