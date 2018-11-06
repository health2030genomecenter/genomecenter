package org.gc.pipelines.util
import java.io.File
import org.gc.pipelines.model._
import org.gc.readqc
import org.gc.readqc.CycleNumberMetrics
import org.nspl._
import org.nspl.awtrenderer._

object ReadQCPlot {

  def make(metrics: Seq[(SampleId, Lane, ReadType, readqc.Metrics)]): File = {
    val baseQPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, readType, metrics), idx) =>
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
          case (sample, lane, read, _) => sample + "." + lane + "." + read
        },
        boxColor = ManualColor(Map(0d -> Color.red, 1d -> Color.blue)),
        ylab = "BaseQ",
        xLabelRotation = math.Pi * -0.4,
        xWidth = 70 fts,
        fontSize = 0.5 fts
      )
    }

    val readNumberPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.readNumber.toDouble,
              idx.toDouble,
              color
            )
        }
      val xnames = metrics.zipWithIndex.map {
        case ((sample, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> point(
          color = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "ReadNumber",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = true,
        yHeight = 60 fts,
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts
      )
    }

    val uniquemersPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.numberOfDistinct13Mers.toDouble,
              idx.toDouble,
              color
            )
        }

      val xnames = metrics.zipWithIndex.map {
        case ((sample, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> point(
          color = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "Distinct 13 mers in prefix",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = true,
        yHeight = 60 fts,
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts
      )
    }

    val gcPerSamplePerLanePerRead = {
      val data = metrics.zipWithIndex
        .map {
          case ((_, _, readType, metrics), idx) =>
            val color = if (readType == 1) 0d else 1d
            (
              metrics.gcFraction,
              idx.toDouble,
              color
            )
        }
      val xnames = metrics.zipWithIndex.map {
        case ((sample, lane, read, _), idx) =>
          (idx.toDouble, sample + "." + lane + "." + read)
      }
      xyplot(
        data -> point(
          color = ManualColor(Map(0d -> Color.red, 1d -> Color.blue))))(
        xlab = "GC%",
        yNumTicks = 0,
        ynames = xnames,
        yCustomGrid = true,
        yHeight = 60 fts,
        xlim = Some((0d, 1d)),
        yLabFontSize = 0.5 fts,
        xLabDistance = 0.3 fts
      )
    }

    def makeCyclesBaseQ(
        metrics: Seq[(SampleId, Lane, ReadType, readqc.Metrics)]) = {
      val samples = metrics.map(_._1).distinct
      val colors = DiscreteColors(samples.size)
      val legend = samples.zipWithIndex
        .map {
          case (sample, idx) =>
            sample -> PointLegend(shapePick(idx), colors(idx.toDouble))
        }
        .sortBy(_._1.toString)
      val sample2Color = samples.zipWithIndex.toMap
      val lines = metrics.flatMap {
        case (sample, _, _, metrics) =>
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
      xyplot(lines: _*)(extraLegend = legend,
                        ylab = "BaseQ",
                        xlab = "Cycle",
                        legendFontSize = 0.9 fts,
                        xWidth = 40 fts,
                        yHeight = 40 fts)
    }

    val cycleBaseQsPlots = makeCyclesBaseQ(metrics)

    val cyclesN = {
      val samples = metrics.map(_._1).distinct
      val colors = DiscreteColors(samples.size)
      val legend = samples.zipWithIndex
        .map {
          case (sample, idx) =>
            sample -> LineLegend(stroke = Stroke(1), colors(idx.toDouble))
        }
        .sortBy(_._1.toString)
      val sample2Color = samples.zipWithIndex.toMap
      val lines = metrics.flatMap {
        case (sample, _, _, metrics) =>
          val meanLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, _, ns) =>
              (cycleIdx.toDouble, ns.toDouble)
          }
          val color = colors(sample2Color(sample).toDouble)
          List(meanLine -> line(color = color, stroke = Stroke(0.5)))
      }
      xyplot(lines: _*)(extraLegend = legend,
                        ylab = "Ns",
                        xlab = "Cycle",
                        xWidth = 40 fts,
                        yHeight = 40 fts)
    }

    val compositePlot =
      group(
        cycleBaseQsPlots,
        cyclesN,
        baseQPerSamplePerLanePerRead,
        group(readNumberPerSamplePerLanePerRead,
              uniquemersPerSamplePerLanePerRead,
              gcPerSamplePerLanePerRead,
              HorizontalStack(Right, 0)),
        VerticalStack(gap = 15d)
      )
    pdfToFile(compositePlot.build)
  }

}
