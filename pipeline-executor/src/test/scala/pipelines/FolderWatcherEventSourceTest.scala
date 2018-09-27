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
import org.gc.pipelines.model.SampleSheet
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
    val sampleSheetFileName = "samplesheetfilename"
    val sampleSheetFileContent = "blabla"
    val runId = "runid"
    val runFolder = new File(watchedFolder, runId)
    val sampleSheetFilePath =
      new File(runFolder, sampleSheetFileName).getAbsolutePath

    Given("a folder watch event source")
    val source =
      FolderWatcherEventSource(folderWhereRunFoldersArePlaced =
                                 watchedFolder.getAbsolutePath,
                               fileSignalingCompletion = fileNameToWatch,
                               sampleSheetFilePath = sampleSheetFilePath).events

    val probe = TestProbe()
    source.to(Sink.actorRef(probe.ref, "completed")).run()
    When("a run folder is created")
    runFolder.mkdir
    And("a sample sheet is created")
    openFileWriter(new File(runFolder, sampleSheetFileName)) { writer =>
      writer.write(sampleSheetFileContent)
    }
    Then("the source should not emit")
    probe.expectNoMessage(3 seconds)
    When("the watched file is created")
    val watchedFile = new File(runFolder, fileNameToWatch)
    openFileWriter(watchedFile)(_ => ())
    Then("the source should emit and the sample sheet should be read")
    probe.expectMsg(
      RunfolderReadyForProcessing(runId,
                                  SampleSheet(sampleSheetFileContent),
                                  runFolder.getAbsolutePath))
    When("the watched file is deleted")
    watchedFile.delete
    Then("the source should not emit")
    probe.expectNoMessage(3 seconds)

  }

}
