package com.github.nechaevv.postgresql.protocol

import akka.util.ByteString

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
case class Packet(messageType: Byte, payload: ByteString)
