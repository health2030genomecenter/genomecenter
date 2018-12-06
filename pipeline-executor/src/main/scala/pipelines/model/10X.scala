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
  def resolve(sampleSheet: SampleSheet.ParsedData): SampleSheet = {
    val samples = sampleSheet.poolingLayout
    val resolvedSamples = samples.flatMap { multiplex =>
      val tenXIndexName = IndexId(multiplex.index1.toString)
      val resolvedIndices = tenXBarcodes.get(tenXIndexName).toSeq.flatten
      if (resolvedIndices.size != 4) {
        logger.warn(s"Could not resolve 10X barcode name $tenXIndexName ")
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
