package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import com.typesafe.scalalogging.StrictLogging

case class SampleSheet(sampleSheetContent: String) {
  def parsed = SampleSheet.parseSampleSheet(this)
}

object SampleSheet extends StrictLogging {

  case class Multiplex(sampleId: SampleId,
                       sampleName: SampleName,
                       project: Project,
                       lane: Lane,
                       index1: Index,
                       index2: Option[Index])

  case class ParsedData(header: Map[String, Option[String]],
                        dataHeader: Seq[String],
                        data: Seq[Seq[String]]) {

    override def toString =
      s"SampleSheet.ParsedData(header=$header, dataHeader=$dataHeader, data = $data)"

    private def positive(integer: Int) =
      if (integer >= 0) Some(integer) else None
    private def cell(line: Seq[String], idx: Int) =
      if (idx < line.size) Some(line(idx)) else None
    private def distinctColumnValues(column: Option[Int]): Seq[String] =
      column match {
        case None => Nil
        case Some(columnIdx) =>
          data.flatMap { line =>
            cell(line, columnIdx).toList
          }.distinct
      }

    private val sampleIdColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Sample_ID"))
    private val sampleNameColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Sample_Name"))
    private val projectColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Sample_Project"))
    private val laneColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Lane"))
    private val index1ColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("index"))
    private val index2ColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("index2"))

    val runId = header.get("Experiment Name").flatten
    val sampleIds = distinctColumnValues(sampleIdColumnIdx).map(SampleId(_))
    val lanes = distinctColumnValues(laneColumnIdx).map(Lane(_))
    val projects = distinctColumnValues(projectColumnIdx).map(Project(_))

    val poolingLayout: Seq[Multiplex] = {
      val parsedLines = for {
        sampleIdColumnIdx <- sampleIdColumnIdx
        sampleNameColumnIdx <- sampleNameColumnIdx
        projectColumnIdx <- projectColumnIdx
        laneColumnIdx <- laneColumnIdx
        index1ColumnIdx <- index1ColumnIdx
      } yield {
        data.map { line =>
          val cellInThisLine: Int => Option[String] = cell(line, _)
          (
            cellInThisLine(sampleIdColumnIdx).map(SampleId(_)),
            cellInThisLine(sampleNameColumnIdx).map(SampleName(_)),
            cellInThisLine(projectColumnIdx).map(Project(_)),
            cellInThisLine(laneColumnIdx).map(Lane(_)),
            cellInThisLine(index1ColumnIdx).map(Index(_)),
            index2ColumnIdx.flatMap(cellInThisLine).map(Index(_))
          )

        }
      }
      parsedLines.toList.flatten.collect {
        case (Some(sampleId),
              Some(sampleName),
              Some(project),
              Some(lane),
              Some(index1),
              index2) =>
          Multiplex(sampleId, sampleName, project, lane, index1, index2)
      }
    }

    def getProjectBySampleId(id: SampleId): Option[Project] =
      poolingLayout.find(_.sampleId == id).map(_.project)

  }

  def parseSampleSheet(sheet: SampleSheet): ParsedData = {
    val lines =
      scala.io.Source.fromString(sheet.sampleSheetContent).getLines.toList

    def getSectionLines(name: String) =
      lines
        .dropWhile(line => !line.startsWith(s"[$name]"))
        .drop(1)
        .takeWhile(line => !line.startsWith("["))

    def getFirstKeyValuePair(line: String) =
      line.split(',').toSeq match {
        case Seq()               => None
        case Seq(key, rest @ _*) => Some((key, rest.headOption))
      }

    val header = getSectionLines("Header")
      .map(getFirstKeyValuePair)
      .collect {
        case Some(pair) => pair
      }
      .toMap

    val dataLines = getSectionLines("Data")
    val dataHeader = dataLines.head.split(',').toList
    val dataContentLines = dataLines.drop(1).map(_.split(',').toSeq)

    ParsedData(
      header = header.filterNot(_._1.isEmpty),
      dataHeader = dataHeader,
      data = dataContentLines
    )

  }

  implicit val encoder: Encoder[SampleSheet] =
    deriveEncoder[SampleSheet]
  implicit val decoder: Decoder[SampleSheet] =
    deriveDecoder[SampleSheet]

}
