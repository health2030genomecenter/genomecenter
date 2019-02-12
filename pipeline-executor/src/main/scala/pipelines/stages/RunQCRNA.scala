package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.gc.pipelines.model._
import scala.concurrent.Future
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.gc.pipelines.util.StableSet

case class RunQCTableRNAInput(fileName: String,
                              samples: StableSet[(AnalysisId, StarResult)])
    extends WithSharedFiles(samples.toSeq.flatMap(_._2.files): _*)

case class RunQCTableRNAResult(html: SharedFile, csv: SharedFile)
    extends WithSharedFiles(html, csv)

object RunQCRNA {

  def makeCsvTable(metrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {
    def mkHeader(elems: Seq[String]) =
      elems
        .mkString(",")

    def line(elems: Seq[String]) =
      elems.mkString(",")

    val lines = metrics
      .sortBy(_._2.project.toString)
      .sortBy(_._2.sampleId.toString)
      .map {
        case (analysisId, starMetrics) =>
          import starMetrics._
          import starMetrics.metrics._

          line(
            Seq(
              project,
              sampleId,
              runId,
              analysisId,
              f"${numberOfReads / 1E6}%10.2fM",
              f"$meanReadLength%13.2f",
              f"${uniquelyMappedReads / 1E6}%10.2fM",
              f"${uniquelyMappedPercentage * 100}%6.2f%%",
              f"${multiplyMappedReads / 1E6}%10.2fM",
              f"${multiplyMappedReadsPercentage * 100}%6.2f%%"
            ))

      }
      .mkString("\n")

    val header = mkHeader(
      List("Proj",
           "Sample",
           "Run",
           "AnalysisId",
           "TotalReads",
           "MeanReadLength",
           "UniquelyMapped",
           "UniquelyMapped%",
           "Multimapped",
           "Multimapped%")
    )

    header + "\n" + lines + "\n"

  }

  def makeHtmlTable(metrics: Seq[(AnalysisId, StarMetrics.Root)]): String = {
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
      .sortBy(_._2.project.toString)
      .sortBy(_._2.sampleId.toString)
      .map {
        case (analysisId, starMetrics) =>
          import starMetrics._
          import starMetrics.metrics._

          line(
            Seq(
              project -> left,
              sampleId -> left,
              runId -> left,
              analysisId -> left,
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
      List("Proj", "Sample", "Run", "AnalysisId"),
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
    AsyncTask[RunQCTableRNAInput, RunQCTableRNAResult]("__runqctable_rna", 1) {
      case RunQCTableRNAInput(fileName, samples) =>
        implicit computationEnvironment =>
          implicit val materializer =
            computationEnvironment.components.actorMaterializer

          for {
            parsedMetrics <- Future.traverse(samples.toSeq) {
              case (analysisId,
                    StarResult(log,
                               run,
                               BamWithSampleMetadata(project, sample, _))) =>
                log.source
                  .runFold(ByteString.empty)(_ ++ _)
                  .map(_.utf8String)
                  .map { content =>
                    (analysisId,
                     StarMetrics.Root(content, project, sample, run))
                  }
            }
            html = makeHtmlTable(parsedMetrics)
            csv = makeCsvTable(parsedMetrics)
            file <- SharedFile(
              Source.single(ByteString(html.getBytes("UTF-8"))),
              fileName + ".star.qc.table.html")
            filecsv <- SharedFile(
              Source.single(ByteString(csv.getBytes("UTF-8"))),
              fileName + ".star.qc.table.csv")
          } yield RunQCTableRNAResult(file, filecsv)
    }
}
object RunQCTableRNAResult {
  implicit val encoder: Encoder[RunQCTableRNAResult] =
    deriveEncoder[RunQCTableRNAResult]
  implicit val decoder: Decoder[RunQCTableRNAResult] =
    deriveDecoder[RunQCTableRNAResult]
}
object RunQCTableRNAInput {
  implicit val encoder: Encoder[RunQCTableRNAInput] =
    deriveEncoder[RunQCTableRNAInput]
  implicit val decoder: Decoder[RunQCTableRNAInput] =
    deriveDecoder[RunQCTableRNAInput]
}
