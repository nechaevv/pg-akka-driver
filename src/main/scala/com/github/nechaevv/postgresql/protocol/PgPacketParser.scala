package com.github.nechaevv.postgresql.protocol

import akka.stream.scaladsl.Framing.FramingException
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString

/**
  * Created by v.a.nechaev on 14.07.2016.
  */
class PgPacketParser extends GraphStage[FlowShape[ByteString, Packet]] {
  val in = Inlet[ByteString]("PgPackerParserStage.in")
  val out = Outlet[Packet]("PgPackerParserStage.out")

  override val shape: FlowShape[ByteString, Packet] = FlowShape(in, out)
  override def toString: String = "PgPacketParser"
  val packetHeaderLength = 4

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    var currentPacket: Option[Packet] = None
    var packetLength = 0
    var buffer: ByteString = ByteString.empty

    override def onPush(): Unit = {
      buffer ++= grab(in)
      doParse()
      pushPacketIfAvailable()
    }

    override def onPull(): Unit = pushPacketIfAvailable()

    private def packetReady = currentPacket filter { packet => packetLength == packet.payload.length }

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
        val (payload, leftover) = buffer splitAt (packetLength - packet.payload.length)
        currentPacket = Some(packet.copy(payload = packet.payload ++ payload))
        buffer = leftover
      case None => if (buffer.length >= packetHeaderLength + 1) {
        val iter = buffer.iterator
        val packetType = iter.getByte
        packetLength = iter.getInt - packetHeaderLength
        val payloadLength = Math.min(packetLength, buffer.length - packetHeaderLength - 1)
        val payload = iter.getByteString(payloadLength)
        val leftoverLength = buffer.length - payloadLength - packetHeaderLength - 1
        val leftover = if (leftoverLength > 0) iter.getByteString(leftoverLength) else ByteString.empty
        currentPacket = Some(Packet(packetType, payload))
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
