package com.typesafe.plugin

import java.io.{OutputStream, InputStream, Closeable}

/**
 * Basic mechanism for managing resource cleanup in
 * an end-of-code-block style, e.g.:
 * <code>
 *   using(new ResourceNeedingCleanup()) { r =>
 *     r.doSomething()
 *   }
 *
 * Input/Output stream cleanup implicits included
 */
object ResourceCleanup {
  trait CanCleanup[-T] { def cleanup(x: T) }

  def using[T, U](resource: T)(block: T => U)(implicit cleaner: CanCleanup[T]): U = {
    try {
      block(resource)
    } finally {
      if (resource != null) cleaner.cleanup(resource)
    }
  }

  object IOStreams {
    trait CanCleanupCloseable[T <: Closeable] extends CanCleanup[T] {
      def cleanup(cl: T) { cl.close() }
    }

    implicit object CanCleanupInputStream extends CanCleanupCloseable[InputStream]

    implicit object CanCleanupOutputStream extends CanCleanupCloseable[OutputStream] {
      override def cleanup(os: OutputStream) {
        super.cleanup(os)
        if (os != null) os.flush()
      }
    }
  }
}
