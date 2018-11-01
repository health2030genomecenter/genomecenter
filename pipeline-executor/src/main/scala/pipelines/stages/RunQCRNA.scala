package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model._
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class RunQCTableRNAInput(runId: RunId, samples: Set[StarResult])
    extends WithSharedFiles(samples.toList.flatMap(_.files): _*)

object RunQCRNA {

  def makeHtmlTable(metrics: Seq[StarMetrics.Root]): String = {
    def mkHeader(elems1: Seq[String], elems: Seq[(String, Boolean)]) =
      s"""
          <thead>
            <tr>
              ${elems1
        .dropRight(1)
        .map { elem =>
          s"""<th style="text-align: left; border-bottom: 1px solid #000;">$elem</th>"""
        }
        .mkString("\n")}

          ${elems1.lastOption.map { elem =>
        s"""<th style="text-align: left; border-bottom: 1px solid #000; padding-right:30px;">$elem</th>"""
      }.mkString}

              ${elems
        .map {
          case (elem, left) =>
            val align = if (left) "left" else "right"
            s"""<th style="text-align: $align; border-bottom: 1px solid #000;">$elem</th>"""
        }
        .mkString("\n")}
            </tr>
          </thead>
          """

    def line(elems: Seq[(String, Boolean)]) =
      s"""
          <tr>
          ${elems
        .map {
          case (elem, left) =>
            val align = if (left) "left" else "right"
            s"""<td style="text-align: $align">$elem</td>"""
        }
        .mkString("\n")}
          </tr>
          """

    val left = true
    val right = false
    val lines = metrics
      .sortBy(_.project.toString)
      .sortBy(_.sampleId.toString)
      .map {
        case starMetrics =>
          import starMetrics._
          import starMetrics.metrics._

          line(
            Seq(
              project -> left,
              sampleId -> left,
              f"${numberOfReads / 1E6}%10.2fM" -> right,
              f"$meanReadLength%13.2f" -> right,
              f"${uniquelyMappedReads / 1E6}%10.2fM" -> right,
              f"${uniquelyMappedPercentage * 100}%6.2f%%" -> right,
              f"${multiplyMappedReads / 1E6}%10.2fM" -> right,
              f"${multiplyMappedReadsPercentage * 100}%6.2f%%" -> right
            ))

      }
      .mkString("\n")

    val header = mkHeader(
      List("Proj", "Sample"),
      List(
        "TotalReads" -> right,
        "MeanReadLength" -> right,
        "UniquelyMapped" -> right,
        "UniquelyMapped%" -> right,
        "Multimapped" -> right,
        "Multimapped%" -> right
      )
    )

    """<!DOCTYPE html><head></head><body><table style="border-collapse: collapse;">""" + header + "\n<tbody>" + lines + "</tbody></table></body>"

  }

  val runQCTable =
    AsyncTask[RunQCTableRNAInput, SharedFile]("__runqctable_rna", 1) {
      case RunQCTableRNAInput(runId, samples) =>
        implicit computationEnvironment =>
          implicit val materializer =
            computationEnvironment.components.actorMaterializer

          for {
            parsedMetrics <- Future.traverse(samples.toSeq) {
              case StarResult(log,
                              BamWithSampleMetadata(project, sample, run, _)) =>
                log.source
                  .runFold(ByteString.empty)(_ ++ _)
                  .map(_.utf8String)
                  .map { content =>
                    StarMetrics.Root(content, project, sample, run)
                  }
            }
            html = makeHtmlTable(parsedMetrics)
            file <- SharedFile(
              Source.single(ByteString(html.getBytes("UTF-8"))),
              runId + ".star.qc.table.html")
          } yield file
    }
}
object RunQCTableRNAInput {
  implicit val encoder: Encoder[RunQCTableRNAInput] =
    deriveEncoder[RunQCTableRNAInput]
  implicit val decoder: Decoder[RunQCTableRNAInput] =
    deriveDecoder[RunQCTableRNAInput]
}
