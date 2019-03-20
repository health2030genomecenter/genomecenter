package org.gc.pipelines

import scala.concurrent.Future

package object util {
  def isMac = System.getProperty("os.name").toLowerCase.contains("mac")
  def isLinux = System.getProperty("os.name").toLowerCase.contains("linux")

  def sequenceEither[A, B](xs: Seq[Either[A, B]]): Either[Seq[A], Seq[B]] = {
    val lefts = xs collect { case Left(x)   => x }
    def rights = xs collect { case Right(x) => x }
    if (lefts.isEmpty) Right(rights) else Left(lefts)
  }

  def parseAsStringList(string: String): Either[String, Seq[String]] = {
    io.circe.parser.parse(string) match {
      case Left(parsingFailure) => Left(parsingFailure.toString)
      case Right(json) =>
        json.asArray match {
          case None =>
            Left(s"expected json array, got $json")
          case Some(array) =>
            if (array.forall(_.isString))
              Right(array.flatMap(_.asString.toList))
            else {
              Left(s"expected json array of strings, got $array")
            }
        }
    }

  }

  def traverseAll[T, K](it: Seq[T])(f: T => Future[K]) = {
    implicit val ec = HighlyParallelExecutionContext
    Future.traverse(it)(f)
  }

  val HighlyParallelExecutionContext = {
    val fjp = new java.util.concurrent.ForkJoinPool(1000)

    scala.concurrent.ExecutionContext.fromExecutorService(fjp)
  }
}
