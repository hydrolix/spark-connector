package io.hydrolix.spark

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
}
