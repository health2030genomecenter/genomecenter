package org.gc.pipelines

import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import tasks._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.StrictLogging

import org.gc.pipelines.application._
import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet

import scala.concurrent.ExecutionContext.Implicits.global

object MyTestKit extends akka.testkit.TestKit(ActorSystem())

class PipelinesApplicationTest
    extends FunSuite
    with GivenWhenThen
    with Matchers {
  test("all migrations should get exercised") {
    scala.io.Source
      .fromInputStream(
        this.getClass.getResourceAsStream("/migration_test_data"))
      .getLines
      .map { line =>
        io.circe.parser
          .parse(line)
          .right
          .get
          .hcursor
          .downField("schemaVersion")
          .as[Int]
          .right
          .get
      }
      .toSeq
      .distinct
      .sorted
      .toList shouldBe (0 to PipelineStateMigrations.migrations.size).toList
  }
  test("file based state logging should work") {
    import better.files.File._
    Given("a file with run data serialized")
    val file = fileutils.TempFile.createTempFile("state")
    val source = new java.io.File(
      this.getClass.getResource("/migration_test_data").getFile).toPath
    better.files.Dsl.cp(source, file.toPath)
    And("loaded into a FilePipelineState")
    val pipelineState = new FilePipelineState(file)

    And("two runs with the same ids ")
    val run = RunfolderReadyForProcessing(
      RunId("fake"),
      Some("fakePath"),
      None,
      RunConfiguration(StableSet.empty, None, StableSet.empty, StableSet.empty)
    )
    val run2 = RunfolderReadyForProcessing(
      RunId("fake"),
      Some("fakePath2"),
      None,
      RunConfiguration(StableSet.empty, None, StableSet.empty, StableSet.empty)
    )
    When("the first run is registered")
    pipelineState.registered(run)

    Then(
      "the FilePipelineState's pastRuns method should return 2 runs, first the one in the file")
    val result = Await.result((new FilePipelineState(file)).pastRuns, 5 seconds)
    result.size shouldBe 2
    result.takeRight(1) shouldBe List(run)
    result.map(_.runId) shouldBe List("", "fake")

    When("The second is invalidated")
    pipelineState.invalidated(run.runId)
    Then("the pastRuns method should return 1 run")
    Await
      .result((new FilePipelineState(file)).pastRuns, 5 seconds)
      .size shouldBe 1

    When("a new run with the same runId as the second is registered")
    pipelineState.registered(run2)
    val result2 =
      Await.result((new FilePipelineState(file)).pastRuns, 5 seconds)
    Then("pastRuns should return two runs")
    result2.size shouldBe 2
    result2.map(_.runId) shouldBe List("", "fake")
    result2.map(_.runFolderPath) shouldBe List(Some("raw_data//"),
                                               Some("fakePath2"))

  }

  test(
    "pipelines application should react if a RunfolderReady event is received") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = false)
    val pipelineState = new InMemoryPipelineState

    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       new TestPipeline)

    val processedRuns = Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(samplesFinished(
                     FakeSampleResult(Project("project1"),
                                      SampleId("sample1"),
                                      RunId("fake2"),
                                      "fake0_0fake1_0fake2_0")),
                   true)
        .runWith(Sink.last),
      atMost = 60 seconds
    )

    processedRuns
      .collect {
        case SampleFinished(_, _, _, _, result) => result
      }
      .toSet
      .filter(_.isDefined)
      .map(_.get.asInstanceOf[FakeSampleResult])
      .filter(_.project == "project1")
      .filter(_.sampleId == "sample1")
      .toSet shouldBe Set(
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake0"),
                       "fake0_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake1"),
                       "fake0_0fake1_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake0_0fake1_0fake2_0")
    )
    taskSystem.shutdown

  }

  test(
    "pipelines application should react if the same RunfolderReady event is received multiple times by replacing the old instance of the run") {

    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 2
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = true)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val testPipeline = new TestPipeline

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline)

    val processedRuns =
      Await.result(
        app.processingFinishedSource
          .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
          .takeWhile(samplesFinished(
                       FakeSampleResult(Project("project1"),
                                        SampleId("sample1"),
                                        RunId("fake"),
                                        "fake_1")),
                     true)
          .runWith(Sink.last),
        atMost = 90 seconds
      )

    processedRuns
      .collect {
        case SampleFinished(_, _, _, _, result) => result
      }
      .toSet
      .filter(_.isDefined)
      .map(_.get.asInstanceOf[FakeSampleResult])
      .filter(_.project == "project1")
      .filter(_.sampleId == "sample1")
      .toSet shouldBe Set(
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake"),
                       "fake_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake"),
                       "fake_1")
    )
    taskSystem.shutdown

  }

  test("pipelines application should not demultiplex the same run prematurely") {

    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 2
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = true)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val testPipeline = new TestPipeline

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline)

    intercept[Exception](
      Await.result(
        app.processingFinishedSource
          .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
          .takeWhile(
            samplesFinished(
              FakeSampleResult(Project("project1"),
                               SampleId("sample1"),
                               RunId("fake"),
                               "fake_2")),
            true
          )
          .runWith(Sink.last),
        atMost = 40 seconds
      ))

  }

  test(
    "pipelines application should not process the same sample twice on project completion") {

    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 2
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = true)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val testPipeline = new TestPipeline

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline)

    Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(
          samplesFinishedAll(Set(
            FakeSampleResult(Project("project1"),
                             SampleId("sample1"),
                             RunId("fake"),
                             "fake_1"),
            FakeSampleResult(Project("project1"),
                             SampleId("sample2"),
                             RunId("fake"),
                             "fake_1"),
            FakeSampleResult(Project("project2"),
                             SampleId("sample1"),
                             RunId("fake"),
                             "fake_1"),
            FakeSampleResult(Project("project2"),
                             SampleId("sample2"),
                             RunId("fake"),
                             "fake_1")
          )),
          true
        )
        .runWith(Sink.last),
      atMost = 90 seconds
    )

    MyTestKit.awaitAssert(
      {
        val completedProjects =
          testPipeline.completedProjects.map(_.toSet).toSet

        completedProjects should contain allOf (Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake"),
                           "fake_1"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake"),
                           "fake_1")
        ), Set(
          FakeSampleResult(Project("project2"),
                           SampleId("sample1"),
                           RunId("fake"),
                           "fake_1"),
          FakeSampleResult(Project("project2"),
                           SampleId("sample2"),
                           RunId("fake"),
                           "fake_1")
        ))

      },
      max = 10 seconds,
    )
    taskSystem.shutdown

  }

  test("pipelines application should replace old runs - pattern 1 2 3 2") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 4
    Given(
      "A sequence of three runs with order 1 2 3 2, i.e. is the middle run is repeated")
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns,
                                            uniform = false,
                                            pattern = List(1, 2, 3, 2))
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       new TestPipeline)

    val processedRuns =
      Await.result(
        app.processingFinishedSource
          .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
          .takeWhile(samplesFinished(
                       FakeSampleResult(Project("project1"),
                                        SampleId("sample1"),
                                        RunId("fake3"),
                                        "fake1_0fake2_1fake3_1")),
                     true)
          .runWith(Sink.last),
        atMost = 60 seconds
      )

    Then(
      "Each run should be processed by taking the previous into account and the when the middle run is sent the second time the third should be reprocessed but not the first")
    processedRuns
      .collect {
        case SampleFinished(_, _, _, _, result) => result
      }
      .toSet
      .filter(_.isDefined)
      .map(_.get.asInstanceOf[FakeSampleResult])
      .filter(_.project == "project1")
      .filter(_.sampleId == "sample1")
      .toSet shouldBe Set(
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake1"),
                       "fake1_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake1_0fake2_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake3"),
                       "fake1_0fake2_0fake3_0"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake1_0fake2_1"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake3"),
                       "fake1_0fake2_1fake3_1")
    )
    taskSystem.shutdown

  }

  test("pipelines application should replace old runs -- pattern 1 1 2") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    Given("A sequence of two runs with the first repeated")
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns,
                                            uniform = false,
                                            pattern = List(1, 1, 2))
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))
    val testPipeline = new TestPipeline

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline)

    Then(
      "The first should be processed twice, after which the second should be processed.")
    Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(
          samplesFinished2(Set(
            FakeSampleResult(Project("project1"),
                             SampleId("sample1"),
                             RunId("fake2"),
                             "fake1_1fake2_1"),
            FakeSampleResult(Project("project1"),
                             SampleId("sample1"),
                             RunId("fake2"),
                             "fake1_1fake2_0")
          )),
          true
        )
        .runWith(Sink.last),
      atMost = 60 seconds
    )
    taskSystem.shutdown

  }

  test("pipelines application should replace old runs even in case of failure ") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 2
    Given("A sequence of two runs with the first repeated")
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns,
                                            uniform = false,
                                            pattern = List(-1, -1))
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       new TestPipeline)

    val processedRuns =
      Await.result(
        app.processingFinishedSource
          .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
          .takeWhile(samplesFinished(
                       FakeSampleResult(Project("project1"),
                                        SampleId("sample1"),
                                        RunId("fake-1"),
                                        "fake-1_0")),
                     true)
          .runWith(Sink.last),
        atMost = 60 seconds
      )

    Then("The first should be processed should be processed on the second try.")
    processedRuns
      .collect {
        case SampleFinished(_, _, _, _, result) => result
      }
      .toSet
      .filter(_.isDefined)
      .map(_.get.asInstanceOf[FakeSampleResult])
      .filter(_.project == "project1")
      .filter(_.sampleId == "sample1")
      .toSet shouldBe Set(
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake-1"),
                       "fake-1_0")
    )
    taskSystem.shutdown

  }

  test(
    "pipelines application should respect the pipeline's `canProcess` method") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns, false)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipelineWhichNeverRuns)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(6 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 0

    taskSystem.shutdown

  }

  test("pipelines application should survive a failing pipeline") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource = new FakeSequencingCompleteEventSource(numberOfRuns, false)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       TestPipelineWhichFails)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(6 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 0
  }

  def samplesFinishedAll(waitFor: Set[FakeSampleResult])(
      s: Seq[Any]): Boolean = {
    val r = waitFor.forall { test =>
      s exists {
        case SampleFinished(_, _, _, _, Some(fsr: FakeSampleResult))
            if test == fsr =>
          true
        case _ => false
      }
    }

    !r

  }

  def samplesFinished2(waitFor: Set[FakeSampleResult])(s: Seq[Any]): Boolean = {
    val r =
      (s find {
        case SampleFinished(_, _, _, _, Some(fsr: FakeSampleResult))
            if waitFor.contains(fsr) =>
          true
        case _ => false
      }).isEmpty
    r
  }
  def samplesFinished(waitFor: FakeSampleResult)(s: Seq[Any]): Boolean = {
    println("Sample finished: " + s)
    val r =
      (s find {
        case SampleFinished(_, _, _, _, Some(fsr)) if fsr == waitFor => true
        case _                                                       => false
      }).isEmpty
    r
  }

}

class FakeSequencingCompleteEventSource(take: Int,
                                        uniform: Boolean,
                                        pattern: List[Int] = Nil)
    extends SequencingCompleteEventSource
    with StrictLogging {

  val runFolder = fileutils.TempFile.createTempFile(".temp")
  runFolder.delete
  runFolder.mkdirs
  val runInfo = new java.io.File(runFolder, "RunInfo.xml")
  new java.io.FileOutputStream(runInfo).close
  val fake = fileutils.TempFile.createTempFile(".temp").getAbsolutePath

  def commands =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing(
          RunId("fake"),
          Some(runFolder.getAbsolutePath),
          None,
          RunConfiguration(StableSet.empty,
                           None,
                           StableSet.empty,
                           StableSet.empty)
        )
      )
      .take(take.toLong)
      .zipWithIndex
      .map {
        case (run, idx) =>
          if (uniform) run
          else if (pattern.isEmpty)
            run.copy(runId = RunId(run.runId + idx))
          else run.copy(runId = RunId(run.runId + pattern(idx.toInt)))
      }
      .map(Append(_))
}

case class FakeDemultiplexed(
    project: Project,
    sampleId: SampleId,
    runId: RunId
)

case class FakeSampleResult(
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    accumulator: String
)

trait FakePipeline extends Pipeline[FakeDemultiplexed, FakeSampleResult, Unit] {

  val counter = scala.collection.mutable.Map[FakeDemultiplexed, Int]()
  val runCounter = scala.collection.mutable.Map[RunId, Int]()
  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[FakeDemultiplexed]] = {
    if (r.runId == "fake-1" && !runCounter.contains(r.runId)) {
      synchronized {
        runCounter.update(r.runId, 0)
      }
      println("Pretending demultiplexing exception.")
      Future.failed(new RuntimeException("boo"))
    } else
      Future.successful(
        List(
          FakeDemultiplexed(Project("project1"), SampleId("sample1"), r.runId),
          FakeDemultiplexed(Project("project1"), SampleId("sample2"), r.runId),
          FakeDemultiplexed(Project("project2"), SampleId("sample1"), r.runId),
          FakeDemultiplexed(Project("project2"), SampleId("sample2"), r.runId)
        )
      )
  }

  def getKeysOfDemultiplexedSample(
      d: FakeDemultiplexed): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)
  def getKeysOfSampleResult(d: FakeSampleResult): (Project, SampleId, RunId) =
    (d.project, d.sampleId, d.runId)

  def processCompletedRun(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents): Future[(RunId, Boolean)] =
    Future.successful {
      samples.head.runId -> true
    }

  lazy val completedProjects =
    scala.collection.mutable.ArrayBuffer[Seq[FakeSampleResult]]()
  def processCompletedProject(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents)
    : Future[(Project, Boolean, Option[Nothing])] =
    Future.successful {
      synchronized {
        completedProjects.append(samples)
      }
      (samples.head.project, true, None)
    }

  def processSample(runConfiguration: RunfolderReadyForProcessing,
                    pastSampleResult: Option[FakeSampleResult],
                    demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future {
      println(s"Pretend processing of $demultiplexedSample")
      Thread.sleep(2000)

      // keep track of how many times the same (run,project,sample) is processed
      synchronized {
        counter.get(demultiplexedSample) match {
          case None =>
            counter.update(demultiplexedSample, 0)
          case Some(c) =>
            counter.update(demultiplexedSample, c + 1)
        }
      }
      Some(
        FakeSampleResult(
          demultiplexedSample.project,
          demultiplexedSample.sampleId,
          demultiplexedSample.runId,
          pastSampleResult
            .map(_.accumulator)
            .getOrElse("") + demultiplexedSample.runId + "_" + counter(
            demultiplexedSample)
        ))
    }

  def canProcess(r: RunfolderReadyForProcessing) = true
}

class TestPipeline extends FakePipeline

object TestPipelineWhichNeverRuns extends FakePipeline {

  override def canProcess(r: RunfolderReadyForProcessing) = false

}

object TestPipelineWhichFails extends FakePipeline {

  override def canProcess(r: RunfolderReadyForProcessing) = true

  def fail = {
    def zero = 0
    1 / zero
  }

  override def processSample(runConfiguration: RunfolderReadyForProcessing,
                             pastSampleResult: Option[FakeSampleResult],
                             demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future.successful {
      fail
      None
    }

}
