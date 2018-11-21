package org.gc.pipelines

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

import scala.concurrent.duration._
import java.io.File

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}

import org.gc.pipelines.application._
import org.gc.pipelines.model._
import fileutils._

class FolderWatcherEventSourceTest
    extends TestKit(ActorSystem("FolderWatcherEventSourceTest"))
    with FunSuiteLike
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test(
    "FolderWatcherEventSource should emit an event when the respective file is created") {
    implicit val materializer = ActorMaterializer()
    val watchedFolder = {
      val tmpFile = TempFile.createTempFile("test")
      tmpFile.delete
      tmpFile.mkdir
      tmpFile
    }

    val fileNameToWatch = "something"
    val configurationFile = "config-runid"
    val configurationFileContent =
      "geneModelGtf=b\nglobalIndexSet=b\nautomatic=true\nreferenceFasta=b\ntargetIntervals=b\nbqsr.knownSites=[]\ndemultiplexing=[]\ndbSnpVcf=b\nvariantEvaluationIntervals=b"
    val runConfiguration = RunConfiguration(configurationFileContent)
    val runId = "runid"
    val runFolder = new File(watchedFolder, runId)

    Given("a folder watch event source")
    val source =
      FolderWatcherEventSource(folderWhereRunFoldersArePlaced =
                                 watchedFolder.getAbsolutePath,
                               fileSignalingCompletion = fileNameToWatch,
                               configFileFolder = runFolder).commands

    val probe = TestProbe()
    source.to(Sink.actorRef(probe.ref, "completed")).run()
    When("a run folder is created")
    runFolder.mkdir
    And("a config file is created")
    openFileWriter(new File(runFolder, configurationFile)) { writer =>
      writer.write(configurationFileContent)
    }
    Then("the source should not emit")
    probe.expectNoMessage(3 seconds)
    When("the watched file is created")
    val watchedFile = new File(runFolder, fileNameToWatch)
    openFileWriter(watchedFile)(_ => ())
    Then("the source should emit and the sample sheet should be read")
    probe.expectMsg(
      RunfolderReadyForProcessing(RunId(runId),
                                  runFolder.getAbsolutePath,
                                  runConfiguration.right.get))
    When("the watched file is deleted")
    watchedFile.delete
    Then("the source should not emit")
    probe.expectNoMessage(3 seconds)

  }

}
