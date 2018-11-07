package org.gc.pipelines.application

import scala.concurrent.Future
import tasks._

trait Pipeline[T] {
  def canProcess(r: RunfolderReadyForProcessing): Boolean
  def execute(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Option[T]]

  def combine(t1: T, t2: T): T

  def aggregateAcrossRuns(state: T)(
      implicit tsc: TaskSystemComponents): Future[Boolean]

  def last(implicit tsc: TaskSystemComponents): Future[T]
  def persist(t: T)(implicit tsc: TaskSystemComponents): Future[T]
}
