package org.gc.pipelines.util

import fileutils.exec
import scala.sys.process._
import com.typesafe.scalalogging.Logger
import scala.concurrent.duration._

object Exec {

  sealed trait OnError
  case object ThrowIfNonZero extends OnError
  case object DoNothing extends OnError

  def bash(logDiscriminator: String,
           atMost: Duration = Duration.Inf,
           onError: OnError = DoNothing)(
      script: String): (List[String], List[String], Int) = {
    val logger = Logger("exec." + logDiscriminator)
    logger.info(s"Will execute bash script >>>>$script<<<<")
    var ls: List[String] = Nil
    var lse: List[String] = Nil
    val processBuilder = Process(Seq("bash", "-c", script))
    val exitCode = exec(processBuilder, atMost) { ln =>
      ls = ln :: ls
      logger.info(ln)
    } { ln =>
      lse = ln :: lse;
      logger.info(ln)
    }
    logger.info(s"$logDiscriminator exit with code $exitCode")
    onError match {
      case DoNothing => ()
      case ThrowIfNonZero =>
        if (exitCode != 0) {
          throw new RuntimeException(
            s"Bash script exited with nonzero exit code: $exitCode. Stdout and stderr follows. \n ${ls.reverse
              .mkString("\n")} \n ${lse.reverse.mkString("\n")}")
        }
    }
    (ls.reverse, lse.reverse, exitCode)
  }
}
