package org.gc.pipelines.application

import akka.stream.scaladsl.Source
import org.gc.pipelines.model.{RunId, Project, AnalysisId}

sealed trait Command

sealed trait AssignmentCommand extends Command
sealed trait RunCommand extends Command

case class Append(runFolder: RunfolderReadyForProcessing) extends RunCommand
case class Delete(runId: RunId) extends RunCommand
case class Assign(project: Project, analysis: AnalysisConfiguration)
    extends AssignmentCommand
case class Unassign(project: Project, analysisId: AnalysisId)
    extends AssignmentCommand

trait SequencingCompleteEventSource {
  def commands: Source[Command, _]

}
