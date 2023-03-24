package io.hydrolix

package object spark {
  implicit class SeqStuff[A](underlying: Seq[A]) {
    def findSingle(f: A => Boolean): Option[A] = {
      underlying.filter(f) match {
        case as: Seq[A] if as.isEmpty => None
        case as: Seq[A] if as.sizeIs == 1 => as.headOption
        case other => sys.error("Multiple elements found when zero or one was expected")
      }
    }
  }
}
