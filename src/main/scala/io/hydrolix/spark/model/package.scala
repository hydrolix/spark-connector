package io.hydrolix.spark

package object model {
  implicit class StringStuff(underlying: String) {
    def noneIfEmpty: Option[String] = {
      if (underlying.isEmpty) None else Some(underlying)
    }
  }
}
