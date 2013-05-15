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

/**
 * provides a redis client and a CachePlugin implementation
 * the cache implementation can deal with the following data types:
 * - classes implement Serializable
 * - String, Int, Boolean and long
 */
class RedisPlugin(app: Application) extends CachePlugin {

  import play.api.Play.current

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

  class PlayObjectInputStream(is: InputStream)(implicit app: play.Application) extends ClassLoaderObjectInputStream(Play.classloader, is)

  /**
   * cacheAPI implementation
   * can serialize, deserialize to/from redis
   * value needs be Serializable (a few primitive types are also supported: String, Int, Long, Boolean)
   */
  lazy val api = new CacheAPI {

    private def withDataOutputStream[U](block: DataOutputStream => U)(implicit out: OutputStream) {
      using (new DataOutputStream(out)) { dos =>
        block(dos)
      }
    }

    private def withObjectOutputStream[U](block: ObjectOutputStream => U)(implicit out: OutputStream) {
      using (new ObjectOutputStream(out)) { oos =>
        block(oos)
      }
    }

    def set(key: String, value: Any, expiration: Int) {

      implicit val bos = new ByteArrayOutputStream

      val prefix = value match {
        case v: String => withDataOutputStream { _.writeUTF(v) }; "string"
        case v: Int => withDataOutputStream { _.writeInt(v) }; "int"
        case v: Long => withDataOutputStream { _.writeLong(v) }; "long"
        case v: Double => withDataOutputStream { _.writeDouble(v) }; "double"
        case v: Boolean => withDataOutputStream { _.writeBoolean(v) }; "boolean"
        case v: Serializable => withObjectOutputStream { _.writeObject(v) }; "oos"
        case _ => throw new IOException("unhandled type: " + value.getClass.getName)
      }

      val redisV = prefix + "-" + String.valueOf(Base64Coder.encode(bos.toByteArray))

      Logger.warn(redisV)

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
      val data: Seq[String] = sedisPool.withJedisClient { _.get(key) }.split("-")

      val bis = new ByteArrayInputStream(Base64Coder.decode(data.last))

      val elem = data.head match {
        case "oos" =>
          using(new ClassLoaderObjectInputStream(Play.classloader, bis)) {
            is => is.readObject
          }
        case x => {
          using(new DataInputStream(bis)) {
            is => x match {
                case "string" => is.readUTF
                case "int" => is.readInt
                case "long" => is.readLong
                case "double" => is.readDouble
                case "boolean" => is.readBoolean
                case _ => throw new IOException("can not recognize value")
              }
          }
        }
      }

      Some(elem)
    }
  }
}
