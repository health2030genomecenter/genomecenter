package org.gc.pipelines.model

import com.typesafe.scalalogging.StrictLogging

object TenX extends StrictLogging {
  val tenXBarcodes: Map[IndexId, Seq[Index]] = {
    val json = {
      val inputStream = getClass.getResourceAsStream("/10xIndices.json")
      val str = scala.io.Source.fromInputStream(inputStream).mkString
      inputStream.close
      str
    }
    case class Pair(value: String, data: String)
    import io.circe.generic.auto._
    val parsed = io.circe.parser.decode[Seq[Pair]](json).right.get
    parsed.map {
      case Pair(name, indices) =>
        IndexId(name) -> indices.split(",").toList.map(Index(_))
    }.toMap
  }
  /* Creates a new sample sheet with 10x indices resolved
   *
   * In a 10X sample sheet regular index fields are occupied with a 10X
   * index name, each of which refer to multiple real indices.
   *
   * This function resolves this relationship and creates a sample sheet
   * which may be used with bcl2fastq to correctly demultiplex the samples
   */
  def resolve(sampleSheet: SampleSheet.ParsedData): SampleSheet = {
    val samples = sampleSheet.poolingLayout
    val resolvedSamples = samples.flatMap { multiplex =>
      val tenXIndexNameCandidate1 = IndexId(multiplex.index1.toString)
      val tenXIndexNameCandidate2 =
        multiplex.index2.map(i => IndexId(i.toString))

      val usedIndexName =
        if (tenXBarcodes.contains(tenXIndexNameCandidate1))
          Some(tenXIndexNameCandidate1)
        else tenXIndexNameCandidate2

      val resolvedIndices = usedIndexName
        .flatMap(indexName => tenXBarcodes.get(indexName))
        .toSeq
        .flatten
      if (resolvedIndices.size != 4) {
        logger.warn(s"Could not resolve 10X barcode name $usedIndexName ")
        Nil
      } else
        resolvedIndices.map {
          case index =>
            multiplex.copy(
              sampleId = SampleId(multiplex.sampleId + "_" + index),
              index1 = index,
              index2 = None)
        }
    }

    val renderedHeader = "[Header]\n" + sampleSheet.header
      .map {
        case (key, value) =>
          (List(key) ++ value.toList).mkString(",")
      }
      .mkString("\n")

    val renderedData = "[Data]\nLane,Sample_ID,Sample_Name,index,Sample_Project\n" + resolvedSamples
      .map { mp =>
        List(mp.lane.toString,
             mp.sampleId,
             mp.sampleName,
             mp.index1,
             mp.project)
          .map(_.toString)
          .mkString(",")
      }
      .mkString("\n")

    SampleSheet(renderedHeader + "\n\n" + renderedData)
  }
}
