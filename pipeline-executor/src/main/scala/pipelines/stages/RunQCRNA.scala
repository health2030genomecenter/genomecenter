package org.gc.pipelines.stages

import org.gc.pipelines.model._

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
              numberOfReads.toString,
              meanReadLength.toString,
              uniquelyMappedReads.toString,
              uniquelyMappedPercentage.toString,
              multiplyMappedReads.toString,
              multiplyMappedReadsPercentage.toString
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

}
