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
      samplesWithFastq: Seq[(Project, SampleId, Set[String])])
      extends ProgressData

  case class DemultiplexedSample(project: Project, sample: SampleId, run: RunId)
      extends ProgressDataWithSampleId

  case class SampleProcessingStarted(project: Project,
                                     sample: SampleId,
                                     runId: RunId)
      extends ProgressDataWithSampleId
  case class SampleProcessingFinished(project: Project,
                                      sample: SampleId,
                                      run: RunId)
      extends ProgressDataWithSampleId
  case class SampleProcessingFailed(project: Project,
                                    sample: SampleId,
                                    run: RunId)
      extends ProgressDataWithSampleId

  case class CoverageAvailable(project: Project,
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
                          run: String,
                          analysis: AnalysisId,
                          vcfPath: String)
      extends ProgressDataWithSampleId

  case class JointCallsAvailable(project: Project,
                                 samples: Set[SampleId],
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
                data
                  .collect {
                    case DemultiplexStarted(run) if run == runId =>
                      "demultiplex started"
                    case DemultiplexFailed(run) if run == runId =>
                      "demultiplex failed"
                    case Demultiplexed(run, samples) if run == runId =>
                      s"demultiplexed ${samples.size}"
                  }
                  .map(_.toString)
                  .mkString("", "\n", "\n")
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
              complete {
                for {
                  data <- getData
                } yield {
                  data
                    .collect {
                      case Demultiplexed(_, samples) =>
                        samples.map(_._1).distinct
                    }
                    .flatten
                    .distinct
                    .mkString("", "\n", "\n")
                }
              }
            } ~
            path("projects" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  val sampleStates: Seq[ProgressData] = data.collect {
                    case Demultiplexed(run, samples)
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
                    case v @ CoverageAvailable(project0, _, _, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ BamAvailable(project0, _, _, _, _)
                        if project0 == project =>
                      List(v)
                    case v @ VCFAvailable(project0, _, _, _, _)
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
                  val files = data.collect {
                    case Demultiplexed(run, samples) =>
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
                  val event: Option[ProgressData] = data.collect {
                    case d: DeliveryListAvailable if d.project == project => d

                  }.lastOption
                  event.asJson.noSpaces
                }
              }
            } ~
            path("coverages" / Segment) { project =>
              complete {
                for {
                  data <- getData
                } yield {
                  data
                    .collect {
                      case CoverageAvailable(project0,
                                             sample,
                                             run,
                                             analysis,
                                             coverage) if project0 == project =>
                        sample + "\t" + run + "\t" + analysis + "\t" + coverage
                    }
                    .distinct
                    .mkString("", "\n", "\n")
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
