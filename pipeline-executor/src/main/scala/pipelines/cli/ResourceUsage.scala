package org.gc.pipelines.cli

import tasks.tracker.QueryLog
import tasks.tracker.QueryLog._
import tasks.shared._
import org.gc.pipelines.model.{SampleId, Project, AnalysisId, RunId}
import java.io.File

object ResourceUsage {

  def topologicalSort(tree: Seq[Node],
                      forwardEdges: Map[String, Seq[Node]]): Seq[Node] = {
    var order = List.empty[Node]
    var marks = Set.empty[String]

    def visit(n: Node): Unit =
      if (marks.contains(n.id)) ()
      else {
        val children = forwardEdges.get(n.id).toSeq.flatten
        children.foreach(visit)
        marks = marks + n.id
        order = n :: order
      }

    tree.foreach { node =>
      if (!marks.contains(node.id)) {
        visit(node)
      }
    }

    order

  }

  val cpuTimeKey = "__cpuTime"
  val wallClockTimeKey = "__wallClockTime"
  val cpuNeedKey = "__cpuNeed"

  def aggregateRuntime(tree: Seq[Node],
                       dependentSiblings: Map[String, List[String]]) = {
    val forwardEdges = tree.groupBy(_.parent)
    val sorted = topologicalSort(tree, forwardEdges).reverse
    val wallClockTime = scala.collection.mutable.Map[String, Double]()
    val cpuNeed = scala.collection.mutable.Map[String, Double]()
    val cpuTime = scala.collection.mutable.Map[String, Double]()

    def max(l: Seq[Double]) = if (l.isEmpty) None else Some(l.max)

    sorted.foreach { node =>
      val children = forwardEdges.get(node.id).toSeq.flatten
      val childrenByTaskId = children.groupBy(_.taskId)

      // graph among siblings
      // edges in this graph are of `dependentSiblings`
      val childrenGraph = children
        .map { ch =>
          val outTaskIds = dependentSiblings.get(ch.taskId).toList.flatten
          val outNodes =
            outTaskIds.flatMap(t => childrenByTaskId.get(t).toSeq).flatten
          (ch.id, outNodes)
        }
        .filter(_._2.nonEmpty)
        .toMap
      val childrenTopologicalOrder =
        topologicalSort(children, childrenGraph).reverse

      childrenTopologicalOrder.foreach { node =>
        val children = childrenGraph.get(node.id).toSeq.flatten
        val wallClockTime1 = wallClockTime(node.id) + max(
          children.map(ch => wallClockTime(ch.id))).getOrElse(0d)
        wallClockTime.update(node.id, wallClockTime1)
      }

      val maxWallClockTimeOfChildren = max(
        children
          .map { ch =>
            wallClockTime(ch.id)
          }).getOrElse(0d)

      val wallClockTime0 = node.elapsedTime.toDouble + maxWallClockTimeOfChildren

      val cpuNeed0 = math.max(node.resource.cpu.toDouble,
                              children.map(ch => cpuNeed(ch.id)).sum)
      val cpuTime0 = node.elapsedTime * node.resource.cpu + children
        .map(ch => cpuTime(ch.id))
        .sum

      wallClockTime.update(node.id, wallClockTime0)
      cpuNeed.update(node.id, cpuNeed0)
      cpuTime.update(node.id, cpuTime0)

    }

    tree.map { node =>
      node.copy(
        labels = node.labels ++ Labels(
          List(
            cpuTimeKey -> (cpuTime(node.id)).toString,
            cpuNeedKey -> cpuNeed(node.id).toString,
            wallClockTimeKey -> (wallClockTime(node.id)).toString
          )))
    }

  }

  def collapseMultiEdges(tree: Seq[Node]) = {
    val by = (n: Node) => (n.taskId -> n.parent)
    val ids = tree.groupBy(_.id).map { case (id, nodes) => (id, nodes.head) }
    tree
      .groupBy(by)
      .toSeq
      .map {
        case ((_, _), group) =>
          val representative = group.head
          val transformedPath = representative.pathFromRoot.map { id =>
            ids.get(id).map(_.taskId).getOrElse("root")
          }

          representative.copy(
            pathFromRoot = transformedPath,
            labels = representative.labels ++ Labels(
              List(multiplicityKey -> group.size.toString))
          )

      }

  }

  def ancestorsFinished(tree: Seq[Node]) = {
    val ids = tree.groupBy(_.id).map { case (id, nodes) => (id, nodes.head) }
    tree.filter(node =>
      node.pathFromRoot.forall(id =>
        ids.contains(id) || node.pathFromRoot.head == id))
  }

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
      analysisId.map(a => labels("analysis") == a).getOrElse(true) &&
      runId.map(a => labels("run") == a).getOrElse(true)
    }

  def plotSample(f: File,
                 project: Project,
                 sampleId: Option[SampleId],
                 analysisId: Option[AnalysisId],
                 runId: Option[RunId],
                 subtree: Option[String],
                 printDot: Boolean) = {
    val tree = readFile(f)
    val onlyFinished = ancestorsFinished(tree)
    val selected =
      selectSample(onlyFinished, project, sampleId, analysisId, runId)
    val selectedTree = subtree match {
      case None    => selected
      case Some(s) => QueryLog.subtree(selected, s)
    }

    val extraDependencies = Map(
      "__merge-markduplicate" -> List("__bwa-perlane"),
      "__sortbam" -> List("__bwa-persample"),
      "__bqsr-train" -> List("__sortbam"),
      "__bqsr-apply" -> List("__bqsr-train"),
      "__alignmentqc-selection" -> List("__bqsr-apply"),
      "__alignmentqc-general" -> List("__bqsr-apply"),
      "__alignmentqc-wgs" -> List("__bqsr-apply"),
      "__haplotypecaller" -> List("__alignmentqc-general",
                                  "__alignmentqc-selection",
                                  "__alignmentqc-wgs"),
      "__genotypegvcfs" -> List("__haplotypecaller"),
      "__collectvariantcallingmetrics" -> List("__genotypegvcfs"),
      "__parse_wgs_coverage" -> List("__alignmentqc-wgs")
    )

    val analysisRootTaskIds = Set("__persample-single")

    val timesComputed = aggregateRuntime(
      selectedTree,
      extraDependencies
    )

    val analysisRoots: Seq[Node] =
      timesComputed.filter(node => analysisRootTaskIds.contains(node.taskId))

    if (printDot) {
      val collapsed = collapseMultiEdges(timesComputed)
      println(
        dot(collapsed,
            extraDependencies.toSeq
              .flatMap(x => x._2.map(y => x._1 -> y))
              .toList))
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

  def dot(s: Seq[Node], extraEdges: List[(String, String)]) = {
    val nodelist = s
      .map { node =>
        val labels = node.labels.values.toMap
        val cpuTime = labels(cpuTimeKey).toDouble
        val wallClockTime = labels(wallClockTimeKey).toDouble
        val cpuNeed = labels(cpuNeedKey).toDouble

        s""""${node.id}" [label="${node.taskId}(${formatTime(
          node.elapsedTime.toDouble)}h,${formatTime(wallClockTime)}wch,${formatTime(
          cpuTime)}ch,${node.resource.cpu}c,${cpuNeed}C)"] """
      }
      .mkString(";")
    val edgelist = s
      .map { node =>
        val multiplicityLabel =
          node.labels.values.toMap.get(multiplicityKey) match {
            case None      => ""
            case Some("1") => ""
            case Some(x)   => "[label= \"" + x + "x\"]"
          }
        s""""${node.parent}" -> "${node.id}" $multiplicityLabel """
      }
      .mkString(";")

    val extraEdgeList = extraEdges
      .map {
        case (from, to) =>
          s""""${from}" -> "${to}" [color="red"] """
      }
      .mkString(";")

    s"""digraph tasks {$nodelist;$edgelist;$extraEdgeList}"""
  }

}
