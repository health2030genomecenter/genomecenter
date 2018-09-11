package org.gc.pipelines.application

import scala.concurrent.Future
import tasks._

trait Pipeline {
  def canProcess(r: RunfolderReadyForProcessing): Boolean
  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Unit]
}
