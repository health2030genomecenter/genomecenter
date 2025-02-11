package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.model._

import akka.actor._
import tasks.TaskSystemComponents
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

sealed trait ProgressData
sealed trait ProgressDataWithSampleId extends ProgressData {
  def sample: SampleId
  def project: Project
}
object ProgressData {
  case class DemultiplexStarted(run: RunId) extends ProgressData
  case class DemultiplexFailed(run: RunId) extends ProgressData
  case class Demultiplexed(
      run: RunId,
      samplesWithFastq: Seq[(Project, SampleId, Set[String])],
      statistics: Seq[(DemultiplexingId, DemultiplexingStats.Root)])
      extends ProgressData

  case class DemultiplexedSample(project: Project, sample: SampleId, run: RunId)
      extends ProgressDataWithSampleId

  case class SampleProcessingStarted(project: Project,
                                     sample: SampleId,
                                     run: RunId)
      extends ProgressDataWithSampleId
  case class SampleProcessingFinished(project: Project,
                                      sample: SampleId,
                                      run: RunId)
      extends ProgressDataWithSampleId
  case class SampleProcessingFailed(project: Project,
                                    sample: SampleId,
                                    run: RunId)
      extends ProgressDataWithSampleId
  case class FastCoverageAvailable(project: Project,
                                   sample: SampleId,
                                   runIdTag: String,
                                   analysis: AnalysisId,
                                   wgsCoverage: Double)
      extends ProgressDataWithSampleId

  case class BamAvailable(project: Project,
                          sample: SampleId,
                          runIdTag: String,
                          analysis: AnalysisId,
                          bamPath: String)
      extends ProgressDataWithSampleId

  case class VCFAvailable(project: Project,
                          sample: SampleId,
                          runIdTag: String,
                          analysis: AnalysisId,
                          vcfPath: String)
      extends ProgressDataWithSampleId

  case class JointCallsStarted(project: Project,
                               analysisId: AnalysisId,
                               samples: Set[SampleId],
                               runs: Set[RunId])
      extends ProgressData
  case class JointCallsAvailable(project: Project,
                                 analysisId: AnalysisId,
                                 samples: Set[SampleId],
                                 runs: Set[RunId],
                                 vcfPath: String)
      extends ProgressData
  case class DeliveryListAvailable(project: Project,
                                   samples: Seq[SampleId],
                                   files: Seq[String],
                                   runsIncluded: Seq[RunId])
      extends ProgressData

  implicit val encoder: Encoder[ProgressData] = deriveEncoder[ProgressData]
  implicit val decoder: Decoder[ProgressData] = deriveDecoder[ProgressData]
}
import ProgressData._

trait SendProgressData {
  def send(data: ProgressData): Unit
}

class ProgressServer(taskSystemActorSystem: ActorSystem)(
    implicit ec: ExecutionContext)
    extends StrictLogging
    with SendProgressData
    with HttpComponent {

  private case object GetState

  val _endpointActor = {
    val props = Props(
      new Actor {
        var state = List.empty[ProgressData]
        def receive = {
          case GetState => sender ! state
          case x: ProgressData =>
            state = x :: state
          case _ =>
        }
      }
    )
    val actorRef = taskSystemActorSystem.actorOf(props, "progress-server")

    logger.info(
      s"Progress server actor created on $actorRef ${actorRef.path} ${actorRef.path.address}")
    actorRef

  }

  logger.info("Progress server started.")

  def send(data: ProgressData) = _endpointActor ! data

  private def getData = {
    implicit val timeOut = akka.util.Timeout(5 seconds)
    (_endpointActor ? GetState).mapTo[Seq[ProgressData]].map(_.reverse)
  }

  val route =
    get {
      pathSingleSlash {
        complete("""|How to use: 
        |GET / 
        |GET /v2/runs/{runid}
        |GET /v2/runs
        |GET /v2/projects
        |GET /v2/projects/{projectname}
        |GET /v2/bams/{projectname}
        |GET /v2/vcfs/{projectname}
        |GET /v2/runconfigurations
        |GET /v2/runconfigurations/{runid}
        |GET /v2/analyses
        |GET /v2/analyses/{projectname}
        |GET /v2/analyses/{projectname}/{analysisid}
        |POST /v2/runs , and post body
        |DELETE /v2/runs/{runid}
        |POST /v2/analyses/{projectname} , and post body
        |DELETE /v2/analyses/{projectname}/{analysisid}""".stripMargin)
      } ~
        pathPrefix("v2") {
          path("runs" / Segment) { runId =>
            complete {
              for {
                data <- getData
              } yield {
                val events: Seq[ProgressData] = data
                  .collect {
                    case v @ DemultiplexStarted(run) if run == runId =>
                      v
                    case v @ DemultiplexFailed(run) if run == runId =>
                      v
                    case v @ Demultiplexed(run, _, _) if run == runId =>
                      v
                  }

                events.asJson.noSpaces
              }
            }
          } ~
            path("runs") {
              complete {
                for {
                  data <- getData
                } yield {
                  data
                    .collect {
                      case DemultiplexStarted(run) => run
                    }
                    .distinct
                    .mkString("", "\n", "\n")
                }
              }
            } ~
            path("projects") {
              parameters("progress".?) { maybeProgressParameter =>
                complete {
                  for {
                    data <- getData
                  } yield {
                    maybeProgressParameter match {
                      case Some(_) =>
                        val events: Seq[ProgressData] = data.collect {
                          case v: Demultiplexed            => v
                          case v: ProgressDataWithSampleId => v
                          case v: JointCallsAvailable      => v
                          case v: JointCallsStarted        => v
                        }
                        events.asJson.noSpaces

                      case None =>
                        data
                          .collect {
                            case Demultiplexed(_, samples, _) =>
                              samples.map(_._1).distinct
                          }
                          .flatten
                          .distinct
                          .mkString("", "\n", "\n")
                    }
                  }
                }
              }
            } ~
            path("projects" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  val sampleStates: Seq[ProgressData] = data.collect {
                    case Demultiplexed(run, samples, _)
                        if samples.map(_._1).contains(project) =>
                      samples
                        .filter(_._1 == project)
                        .map(p =>
                          DemultiplexedSample(Project(project), p._2, run))
                        .toSeq
                    case v @ SampleProcessingStarted(project0, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ SampleProcessingFailed(project0, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ SampleProcessingFinished(project0, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ BamAvailable(project0, _, _, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ VCFAvailable(project0, _, _, _, _)
                        if project0 == project =>
                      List(v)
                      List(v)
                    case v @ FastCoverageAvailable(project0, _, _, _, _)
                        if project0 == project =>
                      List(v)
                  } flatten

                  sampleStates.asJson.noSpaces

                }
              }
            } ~
            path("fastqs" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  val events = data.collect {
                    case ev: Demultiplexed => ev
                  }

                  val files =
                    events.groupBy(_.run).toSeq.map(_._2.last).collect {
                      case Demultiplexed(run, samples, _) =>
                        val samplesOfProject = samples.filter {
                          case (project0, _, _) => project0 == project
                        }

                        samplesOfProject
                          .flatMap {
                            case (project, sample, fastqs) =>
                              fastqs.map { fastq =>
                                (run, project, sample, fastq)
                              }
                          }

                    }
                  files.flatten
                    .sortBy(_._1.toString)
                    .sortBy(_._3.toString)
                    .map {
                      case (run, project, sample, fastq) =>
                        run + "\t" + project + "\t" + sample + "\t" + fastq
                    }
                    .mkString("", "\n", "\n")

                }
              }

            } ~
            path("bams" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  data
                    .collect {
                      case BamAvailable(project0, sample, run, analysis, path)
                          if project0 == project =>
                        sample + "\t" + run + "\t" + analysis + "\t" + path
                    }
                    .distinct
                    .mkString("", "\n", "\n")
                }
              }
            } ~
            path("vcfs" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  data
                    .collect {
                      case VCFAvailable(project0, sample, run, analysis, path)
                          if project0 == project =>
                        sample + "\t" + run + "\t" + analysis + "\t" + path
                    }
                    .distinct
                    .mkString("", "\n", "\n")
                }
              }
            } ~
            path("deliveries" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  val allDeliveryLists: Seq[DeliveryListAvailable] =
                    data.collect {
                      case d: DeliveryListAvailable if d.project == project => d

                    }
                  val chosen: Option[ProgressData] =
                    allDeliveryLists.toList match {
                      case Nil           => None
                      case single :: Nil => Some(single)
                      case multiple =>
                        logger.info(
                          "Multiple delivery lists are available. Choosing the one with the most runs.")
                        val mostRuns =
                          multiple.sortBy(_.runsIncluded.size).reverse.head
                        // check that this list encloses all other
                        multiple.foreach { otherList =>
                          val missing = otherList.runsIncluded.toSet &~ mostRuns.runsIncluded.toSet
                          if (missing.nonEmpty) {
                            logger.error(
                              s"Multiple non overlapping delivery lists found. $allDeliveryLists")
                          }
                        }
                        Some(mostRuns)
                    }
                  chosen.asJson.noSpaces
                }
              }
            } ~
            path("coverages" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  val events: Seq[ProgressData] = data
                    .filter {
                      case FastCoverageAvailable(project0, _, _, _, _)
                          if project0 == project =>
                        true
                      case _ => false
                    }
                  events.distinct.asJson.noSpaces
                }
              }
            }
        }
    }

}

object ProgressServer extends StrictLogging {
  def send(data: ProgressData)(implicit tsc: TaskSystemComponents): Unit = {
    tsc.tasksConfig.masterAddress match {
      case None =>
        logger.error("No address to send progress data to.")
      case Some(masterAddress) =>
        val remoteActorPath =
          s"akka.tcp://tasks@${masterAddress.getHostName}:${masterAddress.getPort}/user/progress-server"
        import tsc.actorsystem.dispatcher
        (for {
          remoteActor <- tsc.actorsystem
            .actorSelection(remoteActorPath)
            .resolveOne(60 seconds)
        } yield {
          remoteActor ! data
        }).failed.foreach {
          case e =>
            logger.error(
              s"Failed to send data to progress server on address: $remoteActorPath",
              e)
        }
    }

  }
}
