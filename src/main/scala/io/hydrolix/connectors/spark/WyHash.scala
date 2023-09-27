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
package io.hydrolix.connectors.spark

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
