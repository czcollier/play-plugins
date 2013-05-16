package com.typesafe.plugin

import play.api._
import org.sedis._
import redis.clients.jedis._
import play.api.cache._
import java.io._
import biz.source_code.base64Coder._
import akka.util.ClassLoaderObjectInputStream

import ResourceCleanup._
import ResourceCleanup.IOStreams._
import org.slf4j.LoggerFactory

/**
 * provides a redis client and a CachePlugin implementation
 * the cache implementation can deal with the following data types:
 * - classes implement Serializable
 * - String, Int, Boolean and long
 */
class RedisPlugin(app: Application) extends CachePlugin {

  val logger = LoggerFactory.getLogger(classOf[RedisPlugin])

  private lazy val host = app.configuration.getString("redis.host").getOrElse("localhost")
  private lazy val port = app.configuration.getInt("redis.port").getOrElse(6379)
  private lazy val timeout = app.configuration.getInt("redis.timeout").getOrElse(2000)
  private lazy val password = app.configuration.getString("redis.password").getOrElse(null)

  /**
   * provides access to the underlying jedis Pool
   */
  lazy val jedisPool = new JedisPool(new JedisPoolConfig(), host, port, timeout, password)

  /**
   * provides access to the sedis Pool
   */
  lazy val sedisPool = new Pool(jedisPool)

  override def onStart() {
    sedisPool
  }

  override def onStop() {
    jedisPool.destroy()
  }

  override lazy val enabled = {
    !app.configuration.getString("redisplugin").filter(_ == "disabled").isDefined
  }

  /**
   * cacheAPI implementation
   * can serialize, deserialize to/from redis
   * value needs be Serializable (a few primitive types are also supported: String, Int, Long, Boolean)
   */
  lazy val api = new CacheAPI {

    private def withDataOutputStream[U](block: DataOutputStream => U) = {
      val bos = new ByteArrayOutputStream
      using (new DataOutputStream(new ByteArrayOutputStream)) { dos => block(dos) }
      bos.toByteArray
    }

    private def withObjectOutputStream[U](block: ObjectOutputStream => U) = {
      val bos = new ByteArrayOutputStream
      using (new ObjectOutputStream(bos)) { oos => block(oos) }
      bos.toByteArray
    }

    private def withDataInputStream[U](block: DataInputStream => U)(implicit in: InputStream) = {
      using (new DataInputStream(in)) { dis => block(dis) }
    }

    private def withObjectInputStream[U](block: ObjectInputStream => U)(implicit in: InputStream) = {
      using (new ClassLoaderObjectInputStream(app.classloader, in)) { ois => block(ois) }
    }

    def set(key: String, value: Any, expiration: Int) {

      val (data, prefix) = value match {
        case v: String => (withDataOutputStream { _.writeUTF(v) }, "string")
        case v: Int => (withDataOutputStream { _.writeInt(v) }, "int")
        case v: Long => (withDataOutputStream { _.writeLong(v) }, "long")
        case v: Double => (withDataOutputStream { _.writeDouble(v) }, "double")
        case v: Boolean => (withDataOutputStream { _.writeBoolean(v) }, "boolean")
        case v: Serializable => (withObjectOutputStream { _.writeObject(v) }, "oos")
        case _ => throw new IOException("unhandled type: " + value.getClass.getName)
      }

      val redisV = prefix + "-" + String.valueOf(Base64Coder.encode(data))

      if (logger.isDebugEnabled)
        logger.debug("inserting: %s => %s".format(key, truncateForLog(redisV, 100)))

      sedisPool.withJedisClient {
        client =>
          client.set(key, redisV)
          if (expiration != 0) client.expire(key, expiration)
      }
    }

    def remove(key: String) {
      sedisPool.withJedisClient {
        client => client.del(key)
      }
    }

    def get(key: String): Option[Any] = {
      val rawData = sedisPool.withJedisClient { _.get(key) }

      if (logger.isDebugEnabled)
        logger.debug("fetched raw data: %s => %s".format(key, truncateForLog(rawData, 100)))

      val (prefix, value) = rawData.split("-") match {
        case Array(t, v) => (t, v)
        case _ => throw new IOException("badly formatted value at key: %s".format(key))
      }

      implicit val bis = new ByteArrayInputStream(Base64Coder.decode(value))

      val elem = prefix match {
        case "string" => withDataInputStream(_.readUTF)
        case "int" => withDataInputStream(_.readInt)
        case "long" => withDataInputStream(_.readLong)
        case "double" => withDataInputStream(_.readDouble)
        case "boolean" => withDataInputStream(_.readBoolean)
        case "oos" => withObjectInputStream(_.readObject)
        case x => throw new IOException("unhandled type: '%s' at key '%s'".format(x, key))
      }

      if (logger.isDebugEnabled)
        logger.debug("fetched: %s: %s".format(elem.getClass.getName, elem))

      Some(elem)
    }

    private def truncateForLog(sv: String, size: Int) = {
      if (sv.length > size)
        "%s ... (truncated to first %d elements)".format(sv.substring(0, size), size)
      else
        sv
    }
  }
}
