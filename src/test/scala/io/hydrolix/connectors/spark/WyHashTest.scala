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

import org.junit.Assert.assertEquals
import org.junit.Test

//noinspection SpellCheckingInspection
class WyHashTest {
  private val testValues = Map(
   "New Jersey" -> "01e4006cfe326967",
   "Staten Island" -> "0b69743766a6ff32",
   "Manhattan" -> "1e56f55b0cbbe386",
   "" -> "42bc986dc5eec4d3",
   "Japanese (日本語)	こんにちは / ｺﾝﾆﾁﾊ" -> "4e89b38aacedbff8",
   "Chinese (中文,普通话,汉语)	你好" -> "4dca661b6a74dc9e",
   "Cantonese (粵語,廣東話)	早晨, 你好" -> "37123e82ec1580c1",
   "Korean (한글)	안녕하세요 / 안녕하십니까" -> "61eb7f47b6fb47bb",
   "Bronx" -> "4d053535cc2bfe5e",
   "Hanunoo (ᜱᜨᜳᜨᜳᜢ)	ᜫᜬᜧ᜴ ᜣᜭᜯᜥ᜴ ᜰᜲᜭᜥ᜴" -> "fa59e3a0ccfa1136",
   "Mongolian (монгол хэл)	Сайн байна уу?" -> "a9ee6f34c21f17df",
   "Northern Thai (ᨣᩣᩴᨾᩮᩬᩥᨦ / ᨽᩣᩈᩣᩃ᩶ᩣ᩠ᨶᨶᩣ)	ᩈ᩠ᩅᩢᩔ᩠ᨯᩦᨣᩕᩢ᩠ᨸ" -> "9f24803dc67df033",
   "Odia (ଓଡ଼ିଆ)	ନମସ୍କାର" -> "8078f3c852c2ab0b",
   "Hindi (हिन्दी)	प्रणाम / पाय लागू" -> "bd74cb772593eeb8",
   "Brooklyn" -> "8b585f55db485a90",
   "Queens" -> "cb3af1db72e86e3c",
   "ASDLFjasdlfkjasDFLKAJsdflasdjfowieuoajoizjodijaf039qj fef;li j2q3p[092 3pq fj; asldkj f;al sdjf a;slkdf" -> "b36e922dc4901cc8",
  )

  @Test
  def doStuff(): Unit = {
    for ((s, expected) <- testValues) {
      val got = WyHash(s)
      assertEquals(s"Hash value of '$s'", expected, got)
    }
  }
}
