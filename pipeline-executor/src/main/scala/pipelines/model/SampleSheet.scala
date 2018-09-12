package org.gc.pipelines.model

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

case class SampleSheet(sampleSheetContent: String) {
  def parsed = SampleSheet.parseSampleSheet(this)
}

object SampleSheet {

  case class Multiplex(sample: SampleId,
                       project: Project,
                       lane: Lane,
                       index1Id: IndexId,
                       index2Id: Option[IndexId])

  case class ParsedData(header: Map[String, Option[String]],
                        dataHeader: Seq[String],
                        data: Seq[Seq[String]],
                        genomeCenterMetadata: Map[String, Option[String]]) {

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
    private val projectColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Sample_Project"))
    private val laneColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("Lane"))
    private val index1ColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("I7_Index_ID"))
    private val index2ColumnIdx: Option[Int] = positive(
      dataHeader.indexOf("I5_Index_ID"))

    val runId = header.get("Experiment Name").flatten
    val sampleIds = distinctColumnValues(sampleIdColumnIdx)
    val lanes = distinctColumnValues(laneColumnIdx)
    val projects = distinctColumnValues(projectColumnIdx)

    val poolingLayout: Seq[Multiplex] = {
      val parsedLines = for {
        sampleIdColumnIdx <- sampleIdColumnIdx
        projectColumnIdx <- projectColumnIdx
        laneColumnIdx <- laneColumnIdx
        index1ColumnIdx <- index1ColumnIdx
      } yield {
        data.map { line =>
          val cellInThisLine: Int => Option[String] = cell(line, _)
          (
            cellInThisLine(sampleIdColumnIdx).map(SampleId(_)),
            cellInThisLine(projectColumnIdx).map(Project(_)),
            cellInThisLine(laneColumnIdx).map(Lane(_)),
            cellInThisLine(index1ColumnIdx).map(IndexId(_)),
            index2ColumnIdx.flatMap(cellInThisLine).map(IndexId(_))
          )

        }
      }
      parsedLines.toList.flatten.collect {
        case (Some(sample), Some(project), Some(lane), Some(index1), index2) =>
          Multiplex(sample, project, lane, index1, index2)
      }
    }
  }

  def parseSampleSheet(sheet: SampleSheet): ParsedData = {
    val lines =
      scala.io.Source.fromString(sheet.sampleSheetContent).getLines.toList

    def getSectionLines(name: String) =
      lines
        .dropWhile(line => !line.startsWith(s"[$name]"))
        .drop(1)
        .takeWhile(line => !line.startsWith("["))

    def getFirstKeyValuePair(line: String) = {
      val Seq(key, rest @ _*) = line.split(',').toSeq
      (key, rest.headOption)
    }
    val header = getSectionLines("Header").map(getFirstKeyValuePair).toMap

    val dataLines = getSectionLines("Data")
    val dataHeader = dataLines.head.split(',').toList
    val dataContentLines = dataLines.drop(1).map(_.split(',').toSeq)

    val genomeCenterMetadata =
      getSectionLines("GenomeCenter").map(getFirstKeyValuePair).toMap

    ParsedData(
      header = header,
      dataHeader = dataHeader,
      data = dataContentLines,
      genomeCenterMetadata = genomeCenterMetadata
    )

  }

  implicit val encoder: Encoder[SampleSheet] =
    deriveEncoder[SampleSheet]
  implicit val decoder: Decoder[SampleSheet] =
    deriveDecoder[SampleSheet]

}
