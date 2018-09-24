package org.gc.pipelines

package object util {
  def isMac = System.getProperty("os.name").toLowerCase.contains("mac")
  def isLinux = System.getProperty("os.name").toLowerCase.contains("linux")

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
}
