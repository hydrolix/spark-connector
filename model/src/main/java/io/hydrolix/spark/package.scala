package io.hydrolix

import org.sparkproject.jetty.util.IO

import java.io.InputStream

package object spark {
  implicit class Slurp(underlying: InputStream) {
    def slurp(): Array[Byte] = {
      IO.readBytes(underlying)
    }
  }
}
