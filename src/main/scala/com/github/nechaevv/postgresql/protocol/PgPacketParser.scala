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
  override def toString: String = "PgPacketParser"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    implicit val byteOrder = ByteOrder.BIG_ENDIAN
    val headerLength = 4

    var currentPacket: Option[Packet] = None
    var buffer: ByteString = ByteString.empty

    override def onPush(): Unit = {
      buffer ++= grab(in)
      doParse()
      pushPacketIfAvailable()
    }

    override def onPull(): Unit = pushPacketIfAvailable()

    private def packetReady = currentPacket filter { packet => packet.length == packet.payload.length + headerLength }

    private def pushPacketIfAvailable(): Unit = if (isAvailable(out)) packetReady match {
      case Some(packet) =>
        push(out, packet)
        currentPacket = None
        doParse()
        if (isClosed(in) && currentPacket.isEmpty) completeStage()
      case None => tryPull()
    }

    override def onUpstreamFinish(): Unit = {
      pushPacketIfAvailable()
      if (currentPacket.isEmpty) completeStage()
    }

    private def doParse(): Unit = currentPacket match {
      case Some(packet) =>
        val (payload, leftover) = buffer splitAt (packet.length - packet.payload.length - headerLength)
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
      if (isClosed(in)) failStage(new FramingException("Upstream finished"))
      else pull(in)
    }

    setHandlers(in, out, this)

  }

}
