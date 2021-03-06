package com.github.nechaevv.postgresql.protocol.frontend

import akka.util.ByteString

/**
  * Created by vn on 17.07.16.
  */
sealed trait FrontendMessage {
  def encode: ByteString
}

abstract class SingletonMessage(packetType: Byte) extends FrontendMessage {
  override def encode: ByteString = encodePacket(packetType, ByteString.empty)
}
abstract class StringMessage(packetType: Byte, payload: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket(packetType, ByteString.newBuilder.putNullTerminatedString(payload).result())
}
case class StartupMessage(database: String, user: String) extends FrontendMessage {
  override def encode: ByteString = {
    val bs = ByteString.newBuilder
      .putInt(protocolVersion << 16)
      .putNullTerminatedString("database").putNullTerminatedString(database)
      .putNullTerminatedString("user").putNullTerminatedString(user)
      .putByte(0).result()
    ByteString.newBuilder.putInt(bs.length + 4).append(bs).result()
  }
}

case class PasswordMessage(password: String) extends StringMessage('p', password)
case class Query(query: String) extends StringMessage('Q', query)
case object Terminate extends SingletonMessage('X')
case object Sync extends SingletonMessage('S')
case object Flush extends SingletonMessage('H')

case class Parse(preparedStatementName: String, query: String, parameterTypes: Seq[Int]) extends FrontendMessage {
  override def encode: ByteString = {
      val builder = ByteString.newBuilder
        .putNullTerminatedString(preparedStatementName)
        .putNullTerminatedString(query)
        .putShort(parameterTypes.length)
      parameterTypes foreach builder.putInt
    encodePacket('P', builder.result())
  }
}

case class Bind(portalName: String, preparedStatementName: String, parameterFormatCodes: Seq[Int],
                parameterValues: Seq[Option[ByteString]], resultColumnFormatCodes: Seq[Int]) extends FrontendMessage {
  override def encode: ByteString = {
    val builder = ByteString.newBuilder
      .putNullTerminatedString(portalName)
      .putNullTerminatedString(preparedStatementName)
      .putShort(parameterFormatCodes.length)
    parameterFormatCodes foreach builder.putShort
    builder.putShort(parameterValues.length)
    parameterValues foreach {
      case Some(pv) =>
        builder.putInt(pv.length)
        builder.append(pv)
      case None => builder.putInt(-1)
    }
    builder.putShort(resultColumnFormatCodes.length)
    resultColumnFormatCodes foreach builder.putShort
    encodePacket('B', builder.result())
  }
}

case class Execute(portal: String, rowLimit: Int) extends FrontendMessage {
  override def encode: ByteString = encodePacket('E', ByteString.newBuilder.putNullTerminatedString(portal).putInt(rowLimit).result())
}

case class DescribeStatement(name: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket('D', ByteString.newBuilder.putByte('S').putNullTerminatedString(name).result())
}
case class DescribePortal(name: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket('D', ByteString.newBuilder.putByte('P').putNullTerminatedString(name).result())
}
case class CloseStatement(name: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket('C', ByteString.newBuilder.putByte('S').putNullTerminatedString(name).result())
}
case class ClosePortal(name: String) extends FrontendMessage {
  override def encode: ByteString = encodePacket('C', ByteString.newBuilder.putByte('P').putNullTerminatedString(name).result())
}
