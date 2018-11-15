package org.gc.pipelines.util

import org.scalatest._
import org.gc.pipelines.model._
import org.gc.readqc.{Metrics, Distribution, CycleNumberMetrics}

class ReadQCPlotTestSuite extends FunSuite {
  test("make readqc plot") {
    new Fixture {
      println(ReadQCPlot.make(data, "titletitletitletitle"))
    }
  }
  trait Fixture {
    val d1 = Distribution(100d, 10d, 50d, 130d)
    val d2 = Distribution(200d, 10d, 150d, 230d)
    val d3 = Distribution(300d, 10d, 250d, 330d)
    val d4 = Distribution(400d, 10d, 350d, 440d)
    val data = Seq(
      (SampleId("s1"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d1,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 19,
         readNumber = 1000L,
         gcFraction = 0.6
       )),
      (SampleId("s2"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d2,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s3"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d3,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s4"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s5"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1300L,
         gcFraction = 0.5
       )),
      (SampleId("s6"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s7"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s8"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d1, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d3, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s9"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d4, 100),
                      CycleNumberMetrics(2, d3, 100),
                      CycleNumberMetrics(3, d2, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       )),
      (SampleId("s10"),
       Lane(1),
       ReadType(1),
       Metrics(
         baseQ = d4,
         cycles = Seq(CycleNumberMetrics(1, d3, 100),
                      CycleNumberMetrics(2, d2, 100),
                      CycleNumberMetrics(3, d1, 200)),
         numberOfDistinct13Mers = 13,
         readNumber = 1000L,
         gcFraction = 0.5
       ))
    )
  }

}
