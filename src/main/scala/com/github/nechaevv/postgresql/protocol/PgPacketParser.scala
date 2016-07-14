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
  val in = Inlet[ByteString]("DelimiterFramingStage.in")
  val out = Outlet[Packet]("DelimiterFramingStage.out")

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
    }

    override def onPull(): Unit = doParse()

    override def onUpstreamFinish(): Unit = completeStage()

    @tailrec
    def doParse(): Unit = {
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
        if (newPacket.length == newPacket.payload.length + headerLength) {
          push(out, newPacket)
          currentPacket = None
          if (isClosed(in)) completeStage()
        } else {
          currentPacket = Some(newPacket)
          tryPull()
        }
        buffer = leftover
        if (buffer.nonEmpty) doParse()
      } else tryPull()
    }

    private def tryPull() = {
      if (isClosed(in)) {
        failStage(new FramingException("Stream finished but there was a truncated final frame in the buffer"))
      } else pull(in)
    }

    setHandlers(in, out, this)

  }

}
