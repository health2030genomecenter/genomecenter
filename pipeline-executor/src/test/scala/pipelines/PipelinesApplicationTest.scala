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

import java.io.File

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.Tag
import org.gc.pipelines.util.ActorSource
import akka.testkit.TestProbe
import akka.stream.DelayOverflowStrategy

object Only extends Tag("only")

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
      .toList shouldBe (0 to PipelineStateMigrations.migrations.size - 1).toList
  }
  test("file based state logging should work") {
    new Fixture {
      import better.files.File._
      Given("a file with run data serialized")
      val file = fileutils.TempFile.createTempFile("state")
      val source = new java.io.File(
        this.getClass.getResource("/migration_test_data").getFile).toPath
      better.files.Dsl.cp(source, file.toPath)
      And("loaded into a FilePipelineState")
      val AS = ActorSystem()
      val pipelineState = new FilePipelineState(file)(AS, AS.dispatcher)

      And("two runs with the same ids ")
      val run = RunfolderReadyForProcessing(
        RunId("fake"),
        Some("fakePath"),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )
      val run2 = RunfolderReadyForProcessing(
        RunId("fake"),
        Some("fakePath2"),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )
      When("the first run is registered")
      pipelineState.registered(run).await

      Then(
        "the FilePipelineState's pastRuns method should return 2 runs, first the one in the file")
      val result =
        Await.result((new FilePipelineState(file)(AS, AS.dispatcher)).pastRuns,
                     5 seconds)
      result.size shouldBe 2
      result.takeRight(1) shouldBe List(
        RunWithAnalyses(run, analysisAssignment))
      result.map(_.runId) shouldBe List("", "fake")

      When("The second is invalidated")
      pipelineState.invalidated(run.runId).await
      Then("the pastRuns method should return 1 run")
      Await
        .result((new FilePipelineState(file)(AS, AS.dispatcher)).pastRuns,
                5 seconds)
        .size shouldBe 1

      When("a new run with the same runId as the second is registered")
      pipelineState.registered(run2).await
      val result2 =
        Await.result((new FilePipelineState(file)(AS, AS.dispatcher)).pastRuns,
                     5 seconds)
      Then("pastRuns should return two runs")
      result2.size shouldBe 2
      result2.map(_.runId) shouldBe List("", "fake")
      result2.map(_.run.runFolderPath) shouldBe List(Some("raw_data//"),
                                                     Some("fakePath2"))

      When("a project is assigned to a configuration")
      pipelineState.assigned(Project("project1"), rnaConfiguration).await
      Then("the state should zip the analysis configuration with future runs")
      Await
        .result(pipelineState.registered(run2), 5 seconds)
        .get shouldBe RunWithAnalyses(
        run2,
        AnalysisAssignments(
          Map(Project("project1") -> Seq(rnaConfiguration),
              Project("project") -> Seq(wesConfigurationFromMigration))))

      When("the configuration is updated")
      pipelineState.assigned(
        Project("project1"),
        rnaConfiguration.copy(referenceFasta = "somethingelse"))
      Then("the state should zip the analysis configuration with future runs")
      Await
        .result(pipelineState.registered(run2), 5 seconds)
        .get shouldBe RunWithAnalyses(
        run2,
        AnalysisAssignments(
          Map(Project("project") -> Seq(wesConfigurationFromMigration),
              Project("project1") -> Seq(
                rnaConfiguration.copy(referenceFasta = "somethingelse"))))
      )

      When("a project is unassigned")
      pipelineState
        .unassigned(Project("project1"), rnaConfiguration.analysisId)
        .await
      Then(
        "the state should not zip the analysis configuration with future runs")
      Await
        .result(pipelineState.registered(run2), 5 seconds)
        .get shouldBe RunWithAnalyses(
        run2,
        AnalysisAssignments(
          Map(Project("project1") -> Seq(),
              Project("project") -> Seq(wesConfigurationFromMigration))))

      AS.terminate()
    }
  }

  test("stable run order in FilePipelineState") {
    new Fixture {
      Given("an empty file ")
      val file = fileutils.TempFile.createTempFile("state")

      val AS = ActorSystem()
      And("loaded into a FilePipelineState")
      val pipelineState = new FilePipelineState(file)(AS, AS.dispatcher)

      And("three runs with two ids ")
      val run1 = RunfolderReadyForProcessing(
        RunId("r1"),
        Some("fakePath"),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )
      val run1b = RunfolderReadyForProcessing(
        RunId("r1"),
        Some("fakePath2"),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )
      val run2 = RunfolderReadyForProcessing(
        RunId("r2"),
        Some("fakePath3"),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )

      When("run1 and run2 are registered")
      pipelineState.registered(run1).await
      pipelineState.registered(run2).await

      Await.result(pipelineState.pastRuns, 5 seconds).map(_.runId) shouldBe Seq(
        "r1",
        "r2")
      Await
        .result(new FilePipelineState(file)(AS, AS.dispatcher).pastRuns,
                5 seconds)
        .map(_.runId) shouldBe Seq("r1", "r2")

      When("run1b is registered")
      pipelineState.registered(run1b).await
      Await.result(pipelineState.pastRuns, 5 seconds).map(_.runId) shouldBe Seq(
        "r1",
        "r2")
      Await
        .result(new FilePipelineState(file)(AS, AS.dispatcher).pastRuns,
                5 seconds)
        .map(_.runId) shouldBe Seq("r1", "r2")

      When("run1b is invalidated")
      pipelineState.invalidated(run1b.runId).await
      And("registered again")
      pipelineState.registered(run1b).await
      Await.result(pipelineState.pastRuns, 5 seconds).map(_.runId) shouldBe Seq(
        "r1",
        "r2")
      Await
        .result(new FilePipelineState(file)(AS, AS.dispatcher).pastRuns,
                5 seconds)
        .map(_.runId) shouldBe Seq("r1", "r2")

      When("run1b and run2 is invalidated")
      pipelineState.invalidated(run1b.runId).await
      pipelineState.invalidated(run2.runId).await
      And("registered again")
      pipelineState.registered(run2).await
      pipelineState.registered(run1b).await
      Await.result(pipelineState.pastRuns, 5 seconds).map(_.runId) shouldBe Seq(
        "r1",
        "r2")
      Await
        .result(new FilePipelineState(file)(AS, AS.dispatcher).pastRuns,
                5 seconds)
        .map(_.runId) shouldBe Seq("r1", "r2")

    }

  }

  test("pipelines application should respect the blacklist") {
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

    val app =
      new PipelinesApplication(eventSource,
                               pipelineState,
                               implicitly[ActorSystem],
                               taskSystem,
                               new TestPipeline,
                               Set((Project("project1"), SampleId("sample1"))))

    val processedRuns = Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(samplesFinished(
                     FakeSampleResult(Project("project2"),
                                      SampleId("sample1"),
                                      RunId("fake2"),
                                      "fake0_0fake1_0fake2_0")),
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
      .toSet shouldBe Set.empty
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)

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
                                       new TestPipeline,
                                       Set())

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
    Await.result(AS.terminate, 5 seconds)

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
                                       testPipeline,
                                       Set())

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
    Await.result(AS.terminate, 5 seconds)
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
                                       testPipeline,
                                       Set())

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
    Await.result(AS.terminate, 5 seconds)
    taskSystem.shutdown
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
                                       testPipeline,
                                       Set())

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
    Await.result(AS.terminate, 5 seconds)
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
                                       new TestPipeline,
                                       Set())

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
        atMost = 120 seconds
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
    Await.result(AS.terminate, 5 seconds)
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
                                       testPipeline,
                                       Set())

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
      atMost = 120 seconds
    )
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)
  }

  test("pipelines application should recover old runs -- pattern 1 1 2", Only) {
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
                                            pattern = List(1, 1, 2),
                                            addDelaySeconds = 0)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))
    val testPipeline = new TestPipeline

    Await.result(eventSource.commands
                   .mapAsync(1) {
                     case Append(run) =>
                       pipelineState.registered((run))
                   }
                   .runForeach(_ => ()),
                 atMost = 90 seconds)

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(new CommandSource {
      val commands = Source.empty ++ Delayed(600)
    }, pipelineState, implicitly[ActorSystem], taskSystem, testPipeline, Set())

    Then(
      "The first should be processed twice, after which the second should be processed.")
    Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(
          samplesFinished2(
            Set(
              FakeSampleResult(Project("project1"),
                               SampleId("sample1"),
                               RunId("fake2"),
                               "fake1_0fake2_0")
            )),
          true
        )
        .runWith(Sink.last),
      atMost = 120 seconds
    )
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)
  }
  test("pipelines application should recover old runs -- pattern 1, 2, 3, 2") {
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
                                            pattern = List(1, 2, 3, 2),
                                            addDelaySeconds = 0)
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))
    val testPipeline = new TestPipeline

    Await.result(eventSource.commands
                   .mapAsync(1) {
                     case Append(run) =>
                       pipelineState.registered((run))
                   }
                   .runForeach(_ => ()),
                 atMost = 90 seconds)

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(new CommandSource {
      val commands = Source.empty ++ Delayed(600)
    }, pipelineState, implicitly[ActorSystem], taskSystem, testPipeline, Set())

    Then(
      "The first should be processed twice, after which the second should be processed.")
    Await.result(
      app.processingFinishedSource
        .scan(Seq.empty[AnyRef])((seq, t) => seq :+ t)
        .takeWhile(
          samplesFinished2(
            Set(
              FakeSampleResult(Project("project1"),
                               SampleId("sample1"),
                               RunId("fake3"),
                               "fake1_0fake2_0fake3_0")
            )),
          true
        )
        .runWith(Sink.last),
      atMost = 120 seconds
    )
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)
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
                                       new TestPipeline,
                                       Set())

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
        atMost = 90 seconds
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
    Await.result(AS.terminate, 5 seconds)
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
                                       TestPipelineWhichFails,
                                       Set())

    val processedRuns = Await.result(app.processingFinishedSource
                                       .takeWithin(6 seconds)
                                       .runWith(Sink.seq),
                                     atMost = 15 seconds)

    processedRuns
      .collect {
        case RunFinished(_, success) => success
      }
      .count(identity) shouldBe 0
    taskSystem.shutdown()
    Await.result(AS.terminate, 5 seconds)
  }

  test("pipelines application should re-execute project completion") {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    Given("A sequence of two runs with the first repeated")
    val eventSource =
      new FakeSequencingCompleteEventSourceWithControl(pattern = List(1, 2, 2))
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))
    val testPipeline = new TestPipeline

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline,
                                       Set())

    val probe = TestProbe()
    app.processingFinishedSource.runWith(Sink.actorRef(probe.ref, None))

    eventSource.send()
    Then("the project should be completed for the first time")
    probe.fishForSpecificMessage(30 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") =>
        samples
          .asInstanceOf[Seq[FakeSampleResult]]
          .map(_.runId)
          .toSet shouldBe Set(RunId("fake1"))
    }

    When("The second run is sent")
    eventSource.send()

    Then("the project should be completed for the second time")
    probe.fishForSpecificMessage(30 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") && (Set(RunId("fake2")) &~ samples
            .asInstanceOf[Seq[FakeSampleResult]]
            .map(_.runId)
            .toSet).isEmpty =>
        samples.asInstanceOf[Seq[FakeSampleResult]].toSet shouldBe Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake2"),
                           "fake1_0fake2_0"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake2"),
                           "fake1_0fake2_0")
        )
    }
    When("The second run is sent again")
    eventSource.send()
    Then(
      "The project should be completed with the first run, and the second analysis of the second run")
    probe.fishForSpecificMessage(30 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") && (Set(RunId("fake2")) &~ samples
            .asInstanceOf[Seq[FakeSampleResult]]
            .map(_.runId)
            .toSet).isEmpty =>
        samples.asInstanceOf[Seq[FakeSampleResult]].toSet shouldBe Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake2"),
                           "fake1_0fake2_1"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake2"),
                           "fake1_0fake2_1")
        )
    }
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)
  }

  test(
    "pipelines application should re-execute project completion on full reprocessing"
  ) {
    implicit val AS = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val config = ConfigFactory.parseString("""
  tasks.cache.enabled = false
    """)
    Given("A sequence of two runs with the first repeated")
    val eventSource =
      new FakeSequencingCompleteEventSourceWithControl(
        pattern = List(1, 2, 1, 2))
    val pipelineState = new InMemoryPipelineState
    val taskSystem = defaultTaskSystem(Some(config))
    val testPipeline = new TestPipeline

    When("Sending these run sequence into a running application")
    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       implicitly[ActorSystem],
                                       taskSystem,
                                       testPipeline,
                                       Set())

    val probe = TestProbe()
    app.processingFinishedSource.runWith(Sink.actorRef(probe.ref, None))

    eventSource.send()
    Then("the project should be completed for the first time")
    probe.fishForSpecificMessage(30 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") =>
        samples
          .asInstanceOf[Seq[FakeSampleResult]]
          .map(_.runId)
          .toSet shouldBe Set(RunId("fake1"))
    }

    When("The second run is sent")
    eventSource.send()

    Then("the project should be completed for the second time")
    probe.fishForSpecificMessage(30 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") && (Set(RunId("fake2")) &~ samples
            .asInstanceOf[Seq[FakeSampleResult]]
            .map(_.runId)
            .toSet).isEmpty =>
        samples.asInstanceOf[Seq[FakeSampleResult]].toSet shouldBe Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake2"),
                           "fake1_0fake2_0"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake2"),
                           "fake1_0fake2_0")
        )
    }
    When("The first run is sent for the second time")
    eventSource.send()
    Then(
      "The project should be completed with the first run, and the second analysis of the second run")
    probe.fishForSpecificMessage(90 seconds) {

      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") && samples
            .asInstanceOf[Seq[FakeSampleResult]]
            .map(_.runId)
            .toSet == Set(RunId("fake2")) =>
        samples.asInstanceOf[Seq[FakeSampleResult]].toSet shouldBe Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake2"),
                           "fake1_1fake2_1"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake2"),
                           "fake1_1fake2_1")
        )
    }
    When("The second run is sent for the second time")
    eventSource.send()
    Then(
      "The project should be completed with the first run, and the second analysis of the second run")
    probe.fishForSpecificMessage(90 seconds) {
      case ProjectFinished(project, true, Some(samples))
          if project == Project("project1") && (Set(RunId("fake2")) &~ samples
            .asInstanceOf[Seq[FakeSampleResult]]
            .map(_.runId)
            .toSet).isEmpty =>
        samples.asInstanceOf[Seq[FakeSampleResult]].toSet shouldBe Set(
          FakeSampleResult(Project("project1"),
                           SampleId("sample1"),
                           RunId("fake2"),
                           "fake1_1fake2_2"),
          FakeSampleResult(Project("project1"),
                           SampleId("sample2"),
                           RunId("fake2"),
                           "fake1_1fake2_2")
        )
    }
    taskSystem.shutdown
    Await.result(AS.terminate, 5 seconds)
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

  trait Fixture {

    implicit class Pimp[T](f: Future[T]) {
      def await = Await.result(f, 5 seconds)
    }

    val wesConfigurationFromMigration =
      WESConfiguration(
        analysisId = AnalysisId("hg38"),
        referenceFasta = "GCA_000001405.15_GRCh38_no_alt_analysis_set.fna",
        targetIntervals =
          "reference_files/sequence_capture/Agilent/sureSelect_Human_all_exons_v7/GRCh38_hg38/S31285117_Covered_formated_sorted.hg38.bed",
        bqsrKnownSites = StableSet(
          "reference_files/variants/dbSNP/GRCh38/b151/gatk/All_20180418.hg38.vcf.gz",
          "reference_files/ftp.broadinstitute.org/bundle/hg38/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz"
        ),
        dbSnpVcf =
          "reference_files/variants/dbSNP/GRCh38/b151/gatk/All_20180418.hg38.vcf.gz",
        variantEvaluationIntervals =
          "reference_files/sequence_capture/Agilent/sureSelect_Human_all_exons_v7/GRCh38_hg38/S31285117_Covered_formated_sorted.hg38.bed",
        vqsrMillsAnd1Kg = None,
        vqsrHapmap = Some(
          "reference_files/ftp.broadinstitute.org/bundle/hg38/hapmap_3.3.hg38.vcf.gz"),
        vqsrOneKgOmni = Some(
          "reference_files/ftp.broadinstitute.org/bundle/hg38/1000G_omni2.5.hg38.vcf.gz"),
        vqsrOneKgHighConfidenceSnps = Some(
          "reference_files/ftp.broadinstitute.org/bundle/hg38/1000G_phase1.snps.high_confidence.hg38.vcf.gz"),
        vqsrDbSnp138 = Some(
          "reference_files/ftp.broadinstitute.org/bundle/hg38/dbsnp_138.hg38.vcf.gz"),
        doVariantCalls = None,
        doJointCalls = Some(true),
        minimumWGSCoverage = Some(20.0),
        minimumTargetCoverage = Some(20.0),
        variantCallingContigs = None,
        singleSampleVqsr = None,
        keepVcf = None,
        mergeSingleCalls = None
      )

    val analysisAssignment = AnalysisAssignments(
      Map(Project("project") -> List(wesConfigurationFromMigration)))

    val referenceFasta = getClass
      .getResource("/tutorial_8017/chr19_chr19_KI270866v1_alt.fasta")
      .getFile

    val gtfFile = new File(
      getClass
        .getResource("/short.gtf")
        .getFile)

    val rnaConfiguration = RNASeqConfiguration(
      analysisId = AnalysisId("default"),
      referenceFasta = referenceFasta,
      geneModelGtf = gtfFile.getAbsolutePath,
      qtlToolsCommandLineArguments = Nil,
      quantificationGtf = gtfFile.getAbsolutePath,
      starVersion = Some("2.6.0a")
    )
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

// provides a fake element with after a custom delay
// needed to prevent the pipeline closing down before the test is complete
// this is a problem in these test because here we use bounded sources (Source.single or Source.tick.take)
object Delayed {
  val runFolder = fileutils.TempFile.createTempFile(".temp")
  runFolder.delete
  runFolder.mkdirs
  def apply(delay: Int) =
    Source
      .single(
        Append(
          RunfolderReadyForProcessing(
            RunId("never"),
            Some(runFolder.getAbsolutePath),
            None,
            RunConfiguration(StableSet.empty, None, StableSet.empty)
          )))
      .delay(delay seconds, DelayOverflowStrategy.emitEarly)
}

class FakeSequencingCompleteEventSource(take: Int,
                                        uniform: Boolean,
                                        pattern: List[Int] = Nil,
                                        addDelaySeconds: Int = 600)
    extends CommandSource
    with StrictLogging {

  val runFolder = fileutils.TempFile.createTempFile(".temp")
  runFolder.delete
  runFolder.mkdirs
  val runInfo = new java.io.File(runFolder, "RunInfo.xml")
  new java.io.FileOutputStream(runInfo).close
  val fake = fileutils.TempFile.createTempFile(".temp").getAbsolutePath

  // concatenating this to the end of the source prevents early completion of the source
  val delay = Delayed(addDelaySeconds)

  def commands =
    Source
      .tick(
        1 seconds,
        2 seconds,
        RunfolderReadyForProcessing(
          RunId("fake"),
          Some(runFolder.getAbsolutePath),
          None,
          RunConfiguration(StableSet.empty, None, StableSet.empty)
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
      .map(Append(_)) ++ delay
}

class FakeSequencingCompleteEventSourceWithControl(pattern: List[Int] = Nil)(
    implicit AS: ActorSystem)
    extends CommandSource
    with StrictLogging {

  val runFolder = fileutils.TempFile.createTempFile(".temp")
  runFolder.delete
  runFolder.mkdirs
  val runInfo = new java.io.File(runFolder, "RunInfo.xml")
  new java.io.FileOutputStream(runInfo).close
  val fake = fileutils.TempFile.createTempFile(".temp").getAbsolutePath

  val cmds = pattern
    .map { idx =>
      RunfolderReadyForProcessing(
        RunId("fake" + idx),
        Some(runFolder.getAbsolutePath),
        None,
        RunConfiguration(StableSet.empty, None, StableSet.empty)
      )
    }
    .map(Append(_))

  val (forwarder, source, closer) = ActorSource.make[Append]

  def commands = source

  private var idx = 0

  def send() = {
    forwarder ! cmds(idx)
    idx += 1
  }

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

trait FakePipeline
    extends Pipeline[FakeDemultiplexed, FakeSampleResult, Seq[FakeSampleResult]] {

  val counter = scala.collection.mutable.Map[FakeDemultiplexed, Int]()
  val runCounter = scala.collection.mutable.Map[RunId, Int]()
  def demultiplex(r: RunfolderReadyForProcessing)(
      implicit tsc: TaskSystemComponents): Future[Seq[FakeDemultiplexed]] = {
    if (r.runId == "fake-1" && !runCounter.contains(r.runId)) {
      synchronized {
        runCounter.update(r.runId, 0)
      }
      println("Pretending demultiplexing exception.")
      Future.failed(new RuntimeException("simulated error"))
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

  def summarizeCompletedSamples(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents): Future[Unit] =
    Future.successful {
      ()
    }

  lazy val completedProjects =
    scala.collection.mutable.ArrayBuffer[Seq[FakeSampleResult]]()
  def processCompletedProject(samples: Seq[FakeSampleResult])(
      implicit tsc: TaskSystemComponents)
    : Future[(Project, Boolean, Option[Seq[FakeSampleResult]])] =
    Future.successful {
      synchronized {
        completedProjects.append(samples)
      }
      (samples.head.project, true, Some(samples))
    }

  def processSample(runConfiguration: RunfolderReadyForProcessing,
                    analysisAssignments: AnalysisAssignments,
                    pastSampleResult: Option[FakeSampleResult],
                    demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future {
      println(s"Pretend processing of $demultiplexedSample")
      Thread.sleep(5000)

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

}

class TestPipeline extends FakePipeline

object TestPipelineWhichFails extends FakePipeline {

  def fail = {
    def zero = 0
    1 / zero
  }

  override def processSample(runConfiguration: RunfolderReadyForProcessing,
                             analysisAssignments: AnalysisAssignments,
                             pastSampleResult: Option[FakeSampleResult],
                             demultiplexedSample: FakeDemultiplexed)(
      implicit tsc: TaskSystemComponents): Future[Option[FakeSampleResult]] =
    Future.successful {
      fail
      None
    }

}
