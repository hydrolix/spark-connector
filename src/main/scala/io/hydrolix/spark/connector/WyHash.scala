package io.hydrolix.spark.connector

import com.sun.jna.{Library, Native}

import java.{lang => jl}

/**
 * See the [README](../../../../../resources/linux-x86-64/README.md)
 */
object WyHash {
  private val instance = Native.load("wyhash", classOf[WyHash])

  def apply(s: String): String = {
    val bytes = s.getBytes("UTF-8")
    val hash = instance.wyhash(bytes, bytes.size, 0L)
    val hex = jl.Long.toUnsignedString(hash, 16)

    if (hex.length == 16) {
      hex
    } else {
      ("0" * (16 - hex.length)) + hex
    }
  }
}

trait WyHash extends Library {
  // uint64_t wyhash(const void *key, uint64_t len, uint64_t seed)
  def wyhash(bytes: Array[Byte], len: Long, seed: Long): Long
}
