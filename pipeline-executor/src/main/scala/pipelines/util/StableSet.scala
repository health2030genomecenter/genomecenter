package org.gc.pipelines.util
import scala.collection.immutable.HashSet
import io.circe.Decoder

/** A set with stable serialization
  *
  * Circe delegates the ordering of a serialized Set to the `iterator` method.
  * HashSet has stable iteration order, but not in its contract
  * The factory method scala.collection.Set.apply returns sets not stable for sizes 2,3,4
  *
  * I did not find a way to override circe's default serializer for collections.
  * This class is an opaque wrapper around HashSet with a custom io.circe.Encoder.
  * The custom encoder reorders the serialized elements lexicographically.
  */
case class StableSet[A](underlying: scala.collection.immutable.HashSet[A])
    extends Traversable[A] {
  override def toSeq = underlying.toSeq
  def contains(a: A) = underlying.contains(a)
  def map[B](f: A => B) = StableSet(underlying.map(f))
  override def find(f: A => Boolean) = underlying.find(f)
  override def filterNot(f: A => Boolean) = StableSet(underlying.filterNot(f))
  def flatMap[B](f: A => scala.collection.GenTraversableOnce[B]) =
    underlying.flatMap(f)
  def ++(that: StableSet[A]) = StableSet(underlying ++ that.underlying)
  def ++(that: Seq[A]) = StableSet(underlying ++ that)
  override def size = underlying.size
  def foreach[U](f: A => U) = underlying.foreach(f)
}

object StableSet {

  import io.circe.Encoder
  import io.circe.Json

  implicit def encoder[A: Encoder]: Encoder[StableSet[A]] = {
    implicit val ordering: Ordering[Json] = Ordering.by(_.noSpaces)
    Encoder
      .encodeSeq[A]
      .contramap[StableSet[A]](_.underlying.toSeq)
      .mapJson(json => Json.arr(json.asArray.get.sorted: _*))
  }

  implicit def decoder[A: Decoder]: Decoder[StableSet[A]] =
    Decoder.decodeSeq[A].map(seq => StableSet(seq: _*))
  def empty[A] = StableSet[A]()
  def apply[A](a: A*): StableSet[A] = StableSet(HashSet(a: _*))

  implicit class syntax[A](set: Set[A]) {
    def toStable = StableSet(set.toSeq: _*)
  }
}
