package com.github.nechaevv.postgresql.protocol

/**
  * Created by v.a.nechaev on 07.07.2016.
  */
case class StartupMessage(user: String, version: Int, parameters: Seq[(String,String)])
