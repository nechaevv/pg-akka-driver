package com.github.nechaevv.postgresql.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

/**
  * Created by v.a.nechaev on 14.07.2016.
  */
object TestSuite {
  implicit lazy val actorSystem = ActorSystem()
  implicit lazy val actorMaterializer = ActorMaterializer()
}
