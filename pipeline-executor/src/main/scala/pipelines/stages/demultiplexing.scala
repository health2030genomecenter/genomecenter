package org.gc.pipelines.stages

import org.gc.pipelines.application.RunfolderReadyForProcessing

import scala.concurrent.Future
import tasks._
import tasks.circesupport._

object Tasks {

  val demultiplexing =
    AsyncTask[RunfolderReadyForProcessing, Int]("demultiplexing", 1) {
      input => implicit computationEnvironment =>
        log.info(s"Pretending that run $input is being processed..")
        Future.successful(1)
    }

}
