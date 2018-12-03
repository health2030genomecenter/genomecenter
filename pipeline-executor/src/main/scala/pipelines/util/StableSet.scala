package org.gc.pipelines.util
import scala.collection.immutable.HashSet
import io.circe.Decoder

/** A set with stable iteration order
  *
  * HashSet has stable iteration order, but not in its contract
  * The factory method scala.collection.Set.apply returns sets not stable for sizes 2,3,4
  *
  * This class is guaranteed to have a stable iteration order up to hashCode collisions.
  * The only important piece here is the `def iterator` the rest is Scala collection boilerplate.
  */
case class StableSet[A](underlying: scala.collection.immutable.HashSet[A])
    extends scala.collection.GenTraversableOnce[A]
    with Set[A]
    with scala.collection.SetLike[A, StableSet[A]] {
  def iterator = underlying.toSeq.sortBy(_.hashCode).iterator
  def contains(a: A) = underlying.contains(a)
  def +(elem: A) = StableSet(underlying + elem)
  def -(elem: A) = StableSet(underlying - elem)
  override def empty = StableSet()
}

object StableSet {

  implicit def canBuildFrom[A] =
    new scala.collection.generic.CanBuildFrom[StableSet[_], A, StableSet[A]] {
      def apply() =
        newBuilder
      def apply(from: StableSet[_]) = newBuilder
    }

  def empty[A] = StableSet[A]()
  def apply[A](a: A*): StableSet[A] = StableSet(HashSet(a: _*))

  implicit class syntax[A](set: Set[A]) {
    def toStable = StableSet(set.toSeq: _*)
  }

  implicit def decoder[A: Decoder]: Decoder[StableSet[A]] =
    Decoder.decodeSeq[A].map(seq => StableSet(seq: _*))

  private def newBuilder[A] =
    scala.collection.mutable
      .ArrayBuffer[A]()
      .mapResult(buf => StableSet(buf: _*))
}
