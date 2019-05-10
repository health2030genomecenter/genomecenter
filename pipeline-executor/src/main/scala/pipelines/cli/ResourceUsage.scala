package org.gc.pipelines.cli

import tasks.tracker.QueryLog
import tasks.tracker.QueryLog._
import org.gc.pipelines.model.{SampleId, Project, AnalysisId, RunId}
import java.io.File

object ResourceUsage {

  def readFile(f: File) =
    fileutils.openFileInputStream(f) { inputStream =>
      QueryLog
        .readNodes(inputStream,
                   excludeTaskIds = Set.empty,
                   includeTaskIds = Set.empty)
        .filter(node =>
          node.labels.values.exists(_._1 == "project") && node.labels.values
            .exists(_._1 == "sample"))

    }

  def selectSample(tree: Seq[QueryLog.Node],
                   project: Project,
                   sampleId: Option[SampleId],
                   analysisId: Option[AnalysisId],
                   runId: Option[RunId]) =
    tree.filter { node =>
      val labels = node.labels.values.toMap
      labels("project") == project && sampleId
        .map(sampleId => labels("sample") == sampleId)
        .getOrElse(true) &&
      analysisId.map(a => labels("analysisId") == a).getOrElse(true) &&
      runId.map(a => labels("run") == a).getOrElse(true)
    }

  def plotSample(f: File,
                 project: Project,
                 sampleId: Option[SampleId],
                 analysisId: Option[AnalysisId],
                 runId: Option[RunId],
                 subtree: Option[String],
                 printDot: Boolean,
                 outFile: Option[String]) = {

    val selectedTree = {
      val tree = readFile(f)
      println(s"File read. Tree size: ${tree.size}")

      val selected =
        selectSample(tree, project, sampleId, analysisId, runId)
      subtree match {
        case None    => selected
        case Some(s) => QueryLog.subtree(selected, s)
      }

      // selected.foreach(println)

      val finished = ancestorsFinished(selected)

      finished
    }
    println(s"Selected nodes ${selectedTree.size}")

    outFile.foreach { outFile =>
      fileutils.openFileWriter(new java.io.File(outFile), false) { writer =>
        selectedTree.foreach { node =>
          import io.circe.syntax._
          writer.write(node.asJson.noSpaces + "\n")
        }
      }
    }

    val analysisRootTaskIds = Set("__persample-single")

    val timesComputed = QueryLog.computeRuntimes(
      selectedTree,
      None
    )

    println("Computed runtimes.")

    val analysisRoots: Seq[Node] =
      timesComputed.filter(node => analysisRootTaskIds.contains(node.taskId))

    if (printDot) {
      println(QueryLog.plotTimes(timesComputed, seconds = true))
    } else {
      val table = analysisRoots
        .sortBy { node =>
          node.labels.values.find(_._1 == "sample").get._2
        }
        .map { node =>
          val labels = node.labels.values.toMap
          List(
            labels("project"),
            labels("sample"),
            labels.get("analysisId").getOrElse(""),
            formatTime(labels(wallClockTimeKey).toDouble) + "wch",
            labels(cpuNeedKey) + "C",
            formatTime(labels(cpuTimeKey).toDouble) + "ch",
            node.id
          ).mkString("\t")
        }
        .mkString("\n") + "\n"
      println(
        "project\tsample\tanalysis\twall-clock-time\tcpus-needed\tcpu-time\tnode-id\n" + table)
    }
  }

  def formatTime(nanos: Double) = {
    val hours = nanos * 1E-9 / 3600
    f"$hours%.2f"
  }

}
