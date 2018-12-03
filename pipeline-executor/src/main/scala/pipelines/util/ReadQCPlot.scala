package org.gc.pipelines.util
import java.io.File
import org.gc.pipelines.model._
import org.gc.readqc
import org.gc.readqc.CycleNumberMetrics
import org.nspl._
import org.nspl.awtrenderer._

object ReadQCPlot {

  def make(
      metrics: Seq[(Project, SampleId, RunId, Lane, ReadType, readqc.Metrics)],
      title: String): File = {
    val baseQPerSamplePerLanePerRead = {
      val plots = metrics.grouped(200).toList.map { metrics =>
        val data = metrics.zipWithIndex
          .map {
            case ((_, _, _, _, readType, metrics), idx) =>
              val color = if (readType == 1) 0d else 1d
              (
                idx.toDouble + .6, //x
                metrics.baseQ.mean, // middle
                metrics.baseQ.mean - metrics.baseQ.sd, // q1
                metrics.baseQ.mean + metrics.baseQ.sd, // q3
                metrics.baseQ.min, // min
                metrics.baseQ.max, // max
                idx.toDouble + 1.4, // x2col
                0d, // fill index
                color // width
              )
          }

        boxplotImpl(
          data,
          xnames = metrics.map {
            case (_, sample, _, lane, read, _) =>
              sample + "." + lane + "." + read
          },
          boxColor = ManualColor(Map(0d -> Color.red, 1d -> Color.blue)),
          ylab = "BaseQ",
          xLabelRotation = math.Pi * -0.4,
          xWidth = 120 fts,
          fontSize = 0.5 fts
        )
      }

      sequence(plots.toList, VerticalStack())
    }

    val maxMeanCoveragePlot = {
      val coverages = metrics
        .groupBy { case (project, sample, _, _, _, _) => (project, sample) }
        .toSeq
        .map {
          case ((project, sample), groups) =>
            val total = groups.map { group =>
              val metrics = group._6
              metrics.readLength.mean * metrics.readNumber
            }.sum
            val hg19GenomeSizeNExcluded = 2897310462L
            val maxMeanCoverage = total / hg19GenomeSizeNExcluded
            (project, sample, maxMeanCoverage)

        }

      val data = coverages.zipWithIndex
        .map {
          case ((_, _, cov), idx) =>
            (
              cov,
              idx.toDouble
            )
        }
      val xnames = coverages.zipWithIndex.map {
        case ((_, sample, _), idx) =>
          (idx.toDouble, sample)
      }
      xyplot(
        data -> bar(
          horizontal = true,
          fill = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "Max mean coverage",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = false,
        yHeight = math.max(60d, data.size.toDouble) fts,
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts,
        yAxisMargin = 0.001,
        ygrid = false,
        xgrid = false,
        xAxisMargin = 0d,
      )
    }

    val readNumberPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, _, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.readNumber.toDouble,
              idx.toDouble,
              color
            )
        }
      val xnames = metrics.zipWithIndex.map {
        case ((_, sample, _, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> bar(
          horizontal = true,
          fill = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "ReadNumber",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = false,
        yHeight = math.max(60d, data.size.toDouble) fts,
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts,
        yAxisMargin = 0.001,
        ygrid = false,
        xgrid = false,
        xAxisMargin = 0d,
      )
    }

    val uniquemersPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, _, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.numberOfDistinct13Mers.toDouble,
              idx.toDouble,
              color
            )
        }

      val xnames = metrics.zipWithIndex.map {
        case ((_, sample, _, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> bar(
          horizontal = true,
          fill = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "Distinct 13 mers in prefix",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = false,
        yHeight = math.max(60d, data.size.toDouble) fts,
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts,
        yAxisMargin = 0.001,
        ygrid = false,
        xgrid = false,
        xAxisMargin = 0d,
      )
    }

    val gcPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, _, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.gcFraction,
              idx.toDouble,
              color
            )
        }
      val xnames = metrics.zipWithIndex.map {
        case ((_, sample, _, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> bar(
          horizontal = true,
          fill = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "GC%",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = false,
        yHeight = math.max(60d, data.size.toDouble) fts,
        xlim = Some((0d, 1d)),
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts,
        yAxisMargin = 0.001,
        ygrid = false,
        xgrid = false,
        xAxisMargin = 0d,
      )
    }

    def makeCyclesBaseQ(
        metrics: Seq[
          (Project, SampleId, RunId, Lane, ReadType, readqc.Metrics)]) = {
      val samples = metrics.map(_._2).distinct
      val colors = DiscreteColors(samples.size)
      val legend = samples.zipWithIndex
        .map {
          case (sample, idx) =>
            sample -> PointLegend(shapePick(idx), colors(idx.toDouble))
        }
        .sortBy(_._1.toString)
      val sample2Color = samples.zipWithIndex.toMap
      val lines = metrics.flatMap {
        case (_, sample, _, _, _, metrics) =>
          val meanLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, baseQ, _) =>
              (cycleIdx.toDouble, baseQ.mean)
          }
          val color = colors(sample2Color(sample).toDouble)
          List(
            meanLine -> line(color = color, stroke = Stroke(0.5)),
            meanLine.grouped(10).map(_.head).toVector -> point(
              shapes = Vector(shapePick(sample2Color(sample))),
              color = color)
          )
      }
      xyplot(lines: _*)(
        extraLegend = legend,
        ylab = "BaseQ",
        xlab = "Cycle",
        legendFontSize = 0.9 fts,
        xWidth = 40 fts,
        yHeight = 40 fts,
        legendLayout =
          ColumnLayout(numRows = 20, horizontalGap = 1d, verticalGap = 1d)
      )
    }

    val cycleBaseQsPlots = makeCyclesBaseQ(metrics)

    val cyclesN = {
      val samples = metrics.map(_._2).distinct
      val colors = DiscreteColors(samples.size)
      val legend = samples.zipWithIndex
        .map {
          case (sample, idx) =>
            sample -> PointLegend(shapePick(idx), colors(idx.toDouble))
        }
        .sortBy(_._1.toString)
      val sample2Color = samples.zipWithIndex.toMap
      val lines = metrics.flatMap {
        case (_, sample, _, _, _, metrics) =>
          val meanLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, _, ns) =>
              (cycleIdx.toDouble, ns.toDouble)
          }
          val color = colors(sample2Color(sample).toDouble)
          List(
            meanLine -> line(color = color, stroke = Stroke(0.5)),
            meanLine.grouped(10).map(_.head).toVector -> point(
              shapes = Vector(shapePick(sample2Color(sample))),
              color = color)
          )
      }
      xyplot(lines: _*)(
        extraLegend = legend,
        ylab = "Ns",
        xlab = "Cycle",
        xWidth = 40 fts,
        yHeight = 40 fts,
        legendLayout =
          ColumnLayout(numRows = 20, horizontalGap = 1d, verticalGap = 1d))
    }

    val compositePlot =
      group(
        TextBox(title, fontSize = 2.0 fts),
        cycleBaseQsPlots,
        cyclesN,
        baseQPerSamplePerLanePerRead,
        group(maxMeanCoveragePlot,
              readNumberPerSamplePerLanePerRead,
              uniquemersPerSamplePerLanePerRead,
              gcPerSamplePerLanePerRead,
              HorizontalStack(Right, 0)),
        VerticalStack(gap = 15d)
      )
    pdfToFile(compositePlot.build)
  }

}
