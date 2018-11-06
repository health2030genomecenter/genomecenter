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
              idx.toDouble + .5, //x
              metrics.baseQ.mean, // middle
              metrics.baseQ.mean - metrics.baseQ.sd, // q1
              metrics.baseQ.mean + metrics.baseQ.sd, // q3
              metrics.baseQ.min, // min
              metrics.baseQ.max, // max
              idx.toDouble + 1.5, // x2col
              0d, // fill index
              color // width
            )
        }

      boxplotImpl(
        data,
        xnames = metrics.map(_._1),
        boxColor = ManualColor(Map(0d -> Color.red, 1d -> Color.blue)),
        ylab = "BaseQ")
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
        yCustomGrid = true
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
        yCustomGrid = true
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
        xlim = Some((0d, 1d))
      )
    }

    val cyclesBaseQ = {
      val colors = DiscreteColors(metrics.size)
      val legend = metrics.zipWithIndex.map {
        case ((sample, lane, read, _), idx) =>
          sample + "." + lane + "." + read -> PointLegend(shape =
                                                            Shape.circle(1),
                                                          colors(idx.toDouble))
      }
      val lines = metrics.zipWithIndex.flatMap {
        case ((_, _, _, metrics), idx) =>
          val meanLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, baseQ, _) =>
              (cycleIdx.toDouble, baseQ.mean)
          }
          val meanMinusSdLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, baseQ, _) =>
              (cycleIdx.toDouble, baseQ.mean - baseQ.sd)
          }
          val color = colors(idx.toDouble)
          List(meanLine -> line(color = color),
               meanMinusSdLine -> line(color = color))
      }
      xyplot(lines: _*)(extraLegend = legend, ylab = "BaseQ", xlab = "Cycle")
    }

    val cyclesN = {
      val colors = DiscreteColors(metrics.size)
      val legend = metrics.zipWithIndex.map {
        case ((sample, lane, read, _), idx) =>
          sample + "." + lane + "." + read -> PointLegend(shape =
                                                            Shape.circle(1),
                                                          colors(idx.toDouble))
      }
      val lines = metrics.zipWithIndex.flatMap {
        case ((_, _, _, metrics), idx) =>
          val meanLine = metrics.cycles.map {
            case CycleNumberMetrics(cycleIdx, _, ns) =>
              (cycleIdx.toDouble, ns.toDouble)
          }
          val color = colors(idx.toDouble)
          List(meanLine -> line(color = color))
      }
      xyplot(lines: _*)(extraLegend = legend, ylab = "Ns", xlab = "Cycle")
    }

    val compositePlot =
      group(
        cyclesBaseQ,
        baseQPerSamplePerLanePerRead,
        cyclesN,
        readNumberPerSamplePerLanePerRead,
        uniquemersPerSamplePerLanePerRead,
        gcPerSamplePerLanePerRead,
        TableLayout(2, 4)
      )
    pdfToFile(compositePlot.build)
  }

}
