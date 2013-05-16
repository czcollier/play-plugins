package com.typesafe.plugin

import org.specs2.mutable._
import play.api.test._
import play.api.test.Helpers._
import play.api.cache.Cache

class RedisPluginTests extends Specification {
  val disableEhcache = Map(("ehcacheplugin" -> "disabled"))

  val redisPlugin = Seq("com.typesafe.plugin.RedisPlugin")

  "Inserting and retrieving a String" should {
    "yield what was inserted" in { running(FakeApplication(
          additionalConfiguration = disableEhcache,
          additionalPlugins = redisPlugin)) {

      import play.api.Play.current
      Cache.set("aKey", "testString")
      val res = Cache.getAs[String]("aKey")

      res.get must equalTo("testString")
    }}
  }
}
