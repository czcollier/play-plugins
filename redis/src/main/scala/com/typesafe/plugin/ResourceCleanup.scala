package com.typesafe.plugin

import java.io.{OutputStream, InputStream, Closeable}

object ResourceCleanup {
  trait CanCleanup[-T] { def cleanup(x: T) }

  trait CanCleanupCloseable[T <: Closeable] extends CanCleanup[T] {
    def cleanup(cl: T) { cl.close() }
  }

  implicit object CanCleanupInputStream extends CanCleanupCloseable[InputStream]

  implicit object CanCleanupOutputStream extends CanCleanupCloseable[OutputStream] {
    override def cleanup(os: OutputStream) { super.cleanup(os); os.flush() }
  }

  def using[T, U](resource: T)(block: T => U)(implicit cleaner: CanCleanup[T]): U = {
    try {
      block(resource)
    } finally {
      if (resource != null) cleaner.cleanup(resource)
    }
  }
}
