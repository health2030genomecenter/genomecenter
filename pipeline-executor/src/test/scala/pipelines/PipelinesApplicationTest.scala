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

class PipelinesApplicationTest
    extends FunSuite
    with GivenWhenThen
    with Matchers {

  test("file based state logging should work") {
    import better.files.File._
    val file = fileutils.TempFile.createTempFile("state")
    val source = new java.io.File(
      this.getClass.getResource("/migration_test_data").getFile).toPath
    better.files.Dsl.cp(source, file.toPath)

    val pipelineState = new FilePipelineState(file)

    val run = RunfolderReadyForProcessing(
      RunId("fake"),
      "fakePath",
      RunConfiguration(StableSet.empty, None, StableSet.empty, StableSet.empty)
    )

    pipelineState.registered(run)

    val result = Await.result((new FilePipelineState(file)).pastRuns, 5 seconds)
    result.size shouldBe 2
    result.take(1) shouldBe List(run)

    pipelineState.invalidated(run.runId)
    Await
      .result((new FilePipelineState(file)).pastRuns, 5 seconds)
      .size shouldBe 1
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

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(23 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 25 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe numberOfRuns

    processedRuns
      .collect {
        case SampleFinished(_, _, _, success, _) => success
      }
      .count(identity) shouldBe 4 * numberOfRuns

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

  }

  test(
    "pipelines application should react if the same RunfolderReady event is received multiple times by replacing the old instance of the run") {

    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    val numberOfRuns = 3
    val eventSource =
      new FakeSequencingCompleteEventSource(numberOfRuns, uniform = true)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       new TestPipeline)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(15 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 16 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 1

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
                       "fake_1"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake"),
                       "fake_2")
    )

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

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(15 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 16 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 4

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
                       RunId("fake3"),
                       "fake1_0fake3_1"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake1_0fake3_1fake2_1")
    )

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

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       new TestPipeline)

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(15 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 16 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 2

    Then(
      "The first should be processed twice, after which the second should be processed.")
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
                       RunId("fake1"),
                       "fake1_1"),
      FakeSampleResult(Project("project1"),
                       SampleId("sample1"),
                       RunId("fake2"),
                       "fake1_1fake2_0")
    )

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
          runFolder.getAbsolutePath,
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

trait FakePipeline extends Pipeline[FakeDemultiplexed, FakeSampleResult] {

  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[FakeDemultiplexed]] =
    Future.successful(
      List(
        FakeDemultiplexed(Project("project1"), SampleId("sample1"), r.runId),
        FakeDemultiplexed(Project("project1"), SampleId("sample2"), r.runId),
        FakeDemultiplexed(Project("project2"), SampleId("sample1"), r.runId),
        FakeDemultiplexed(Project("project2"), SampleId("sample2"), r.runId)
      )
    )

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

  def processCompletedProject(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents): Future[(Project, Boolean)] =
    Future.successful {
      samples.head.project -> true
    }

  val counter = scala.collection.mutable.Map[FakeDemultiplexed, Int]()

  def processSample(runConfiguration: RunfolderReadyForProcessing,
                    pastSampleResult: Option[FakeSampleResult],
                    demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future {
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
