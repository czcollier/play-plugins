package com.typesafe.plugin

import play.api._
import org.sedis._
import redis.clients.jedis._
import play.api.cache._
import java.util._
import java.io._
import biz.source_code.base64Coder._
import akka.util.ClassLoaderObjectInputStream
import scala.collection.immutable.MapLike

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


  type Flushable = Closeable { def flush() }

  trait CanCleanup[-T] { def cleanup(x: T) }

  implicit object CanCleanupCloseable extends CanCleanup[Closeable] {
    def cleanup(cl: Closeable) { cl.close() }
  }

//  implicit object CanCleanupFlushable extends CanCleanup[Flushable] {
//    def cleanup(fl: Flushable) { fl.close(); fl.flush() }
//  }

  def using[T, U](resource: T)(block: T => U)(implicit cleaner: CanCleanup[T]): U = {
    try {
      block(resource)
    } finally {
      if (resource != null) cleaner.cleanup(resource)
    }
  }

  class PlayObjectInputStream(is: InputStream)(implicit app: play.Application) extends ClassLoaderObjectInputStream(Play.classloader, is)

  /**
   * cacheAPI implementation
   * can serialize, deserialize to/from redis
   * value needs be Serializable (a few primitive types are also supported: String, Int, Long, Boolean)
   */
  lazy val api = new CacheAPI {

    def set(key: String, value: Any, expiration: Int) {

      try {
        val baos = new ByteArrayOutputStream()

        val prefix = value match {
          case ov: Serializable => using(new ObjectOutputStream(baos)) {
            oos => oos.writeObject(ov); "oos"
          }
          case x => {
            using(new DataOutputStream(baos)) {
              dos =>
                x match {
                  case v: String => dos.writeUTF(v); "string"
                  case v: Int => dos.writeInt(v); "int"
                  case v: Long => dos.writeLong(v); "long"
                  case v: Boolean => dos.writeBoolean(v); "boolean"
                  case _ => throw new IOException("could not serialize: " + value.toString)
                }
            }
          }
        }
        val redisV = prefix + "-" + String.valueOf(Base64Coder.encode(baos.toByteArray))

        Logger.warn(redisV)

        sedisPool.withJedisClient {
          client =>
            client.set(key, redisV)
            if (expiration != 0) client.expire(key, expiration)
        }
      }
      catch {
        case ex: IOException =>
          Logger.warn("could not serialize key:" + key + " and value:" + value.toString + " ex:" + ex.toString)
      }
    }

    def remove(key: String) {
      sedisPool.withJedisClient {
        client => client.del(key)
      }
    }

    def get(key: String): Option[Any] = {
      try {
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
                  case "boolean" => is.readBoolean
                  case _ => throw new IOException("can not recognize value")
                }
            }
          }
        }

        Some(elem)

      }
      catch {
        case ex: Exception =>
          Logger.warn("could not deserialisze key:" + key + " ex:" + ex.toString)
          ex.printStackTrace()
          None
      }
    }
  }
}
