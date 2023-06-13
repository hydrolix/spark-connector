package io.hydrolix.spark

import com.google.common.io.ByteStreams

import scala.sys.process.{Process, ProcessIO}

package object connector {
  def nope() = throw new UnsupportedOperationException("Hydrolix connector is read-only")

  implicit class SeqStuff[A](underlying: Seq[A]) {
    def findSingle(f: A => Boolean): Option[A] = {
      underlying.filter(f) match {
        case as: Seq[A] if as.isEmpty => None
        case as: Seq[A] if as.size == 1 => as.headOption
        case _ => sys.error("Multiple elements found when zero or one was expected")
      }
    }
  }

  def spawn(args: String*): (Int, String, String) = {
    var stdout: Array[Byte] = null
    var stderr: Array[Byte] = null
    val proc = Process(args).run(new ProcessIO(
      _.close(),
      out => stdout = ByteStreams.toByteArray(out),
      err => stderr = ByteStreams.toByteArray(err)
    ))

    (proc.exitValue(), new String(stdout).trim, new String(stderr).trim)
  }
}
