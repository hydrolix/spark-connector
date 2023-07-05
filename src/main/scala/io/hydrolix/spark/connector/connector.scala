/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hydrolix.spark

import com.google.common.io.ByteStreams

import scala.sys.process.{Process, ProcessIO}

package object connector {
  def nope() = throw new UnsupportedOperationException("Hydrolix connector is read-only")

  implicit class SeqStuff[A](underlying: Seq[A]) {
    def findSingle(f: A => Boolean, what: String = ""): Option[A] = {
      underlying.filter(f) match {
        case as: Seq[A] if as.isEmpty => None
        case as: Seq[A] if as.size == 1 => as.headOption
        case _ => sys.error(s"Multiple ${what + " "}elements found when zero or one was expected")
      }
    }

    def findExactlyOne(f: A => Boolean, what: String): A = {
      findSingle(f, what).getOrElse(sys.error(s"Expected to find exactly one $what"))
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
