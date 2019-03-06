package org.gc.pipelines.application

import com.typesafe.scalalogging.StrictLogging
import org.gc.pipelines.model._
import akka.stream.Materializer

import akka.actor._
import tasks.TaskSystemComponents
import scala.concurrent.duration._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.pattern.ask

sealed trait ProgressData
object ProgressData {
  case class DemultiplexStarted(run: RunId) extends ProgressData
  case class DemultiplexFailed(run: RunId) extends ProgressData
  case class Demultiplexed(
      run: RunId,
      samplesWithFastq: Seq[(Project, SampleId, Set[String])])
      extends ProgressData

  case class SampleProcessingStarted(project: Project, sample: SampleId)
      extends ProgressData
  case class SampleProcessingFinished(project: Project, sample: SampleId)
      extends ProgressData
  case class SampleProcessingFailed(project: Project, sample: SampleId)
      extends ProgressData

  case class CoverageAvailable(project: Project,
                               sample: SampleId,
                               wgsCoverage: Double)
      extends ProgressData

  case class BamAvailable(project: Project, sample: SampleId, bamPath: String)
      extends ProgressData

  case class VCFAvailable(project: Project, sample: SampleId, vcfPath: String)
      extends ProgressData

  case class JointCallsAvailable(project: Project,
                                 samples: Set[SampleId],
                                 vcfPath: String)
      extends ProgressData
}
import ProgressData._

class ProgressServer(implicit MAT: Materializer, AS: ActorSystem)
    extends StrictLogging {

  // Source
  //   .actorRef[ProgressData](bufferSize = 100000,
  //                           overflowStrategy = OverflowStrategy.fail)
  //   .scan(List.empty[ProgressData])((acc, elem) => elem :: acc)
  //   .toMat(Sink.foreach(println))(Keep.left)
  //   .run
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
    AS.actorOf(props, "progress-server")
  }

  logger.info("Progress server started.")

  def send(data: ProgressData) = _endpointActor ! data

  import AS.dispatcher

  private def getData = {
    implicit val timeOut = akka.util.Timeout(5 seconds)
    (_endpointActor ? GetState).mapTo[Seq[ProgressData]]
  }

  private[pipelines] val route =
    get {
      path("/") {
        complete("""
        How to use: 
        GET / 
        GET /run/{runid}
        GET /runs
        GET /projects
        GET /project/{projectname}
        GET /bams/{projectname}
        GET /vcfs/{projectname}
        """)
      } ~
        path("run" / Segment) { runId =>
          complete {
            for {
              data <- getData
            } yield {
              data
                .collect {
                  case d @ DemultiplexStarted(run) if run == runId => d
                  case d @ DemultiplexFailed(run) if run == runId  => d
                  case d @ Demultiplexed(run, _) if run == runId   => d
                }
                .map(_.toString)
                .mkString("\n")
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
                .flatten
                .mkString("\n")
            }
          }
        }
      path("projects") {
        complete {
          for {
            data <- getData
          } yield {
            data
              .collect {
                case Demultiplexed(_, samples) => samples.map(_._1).distinct
              }
              .flatten
              .mkString("\n")
          }
        }
      } ~
        path("project" / Segment) { project =>
          complete {
            for {
              data <- getData
            } yield {
              val sampleStates = data
                .collect {
                  case Demultiplexed(_, samples)
                      if samples.map(_._1).contains(project) =>
                    samples
                      .filter(_._1 == project)
                      .map(p => (p._2, "demultiplexed"))
                  case SampleProcessingStarted(project0, sample)
                      if project0 == project =>
                    List(sample -> "start")
                  case SampleProcessingFailed(project0, sample)
                      if project0 == project =>
                    List(sample -> "fail")
                  case SampleProcessingFinished(project0, sample)
                      if project0 == project =>
                    List(sample -> "finish")
                  case CoverageAvailable(project0, sample, _)
                      if project0 == project =>
                    List(sample -> "cov")
                  case BamAvailable(project0, sample, _)
                      if project0 == project =>
                    List(sample -> "bam")
                  case VCFAvailable(project0, sample, _)
                      if project0 == project =>
                    List(sample -> "vcf")
                }

              sampleStates.flatten
                .groupBy(_._1)
                .toSeq
                .sortBy(_._1.toString)
                .map {
                  case (sample, states) =>
                    sample + "\t" + states.map(_._2).mkString(":")
                }
                .mkString("\n")
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
                  case BamAvailable(project0, sample, path)
                      if project0 == project =>
                    sample + "\t" + path
                }
                .mkString("\n")
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
                  case VCFAvailable(project0, sample, path)
                      if project0 == project =>
                    sample + "\t" + path
                }
                .mkString("\n")
            }
          }
        }
    }

  val port = 9999

  private val bindingFuture =
    Http().bindAndHandle(route, "0.0.0.0", port)

  bindingFuture.andThen {
    case scala.util.Success(serverBinding) =>
      logger.info(
        s"Progress web server is listening on ${serverBinding.localAddress}")
  }(AS.dispatcher)

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
