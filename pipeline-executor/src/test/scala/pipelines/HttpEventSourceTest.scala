package org.gc.pipelines

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

import scala.concurrent.duration._
import java.io.File

import akka.stream.scaladsl.Sink
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model.{
  HttpRequest,
  HttpMethods,
  Uri,
  HttpEntity,
  StatusCodes,
  ContentTypes
}

import org.gc.pipelines.application._
import org.gc.pipelines.application.dto._
import org.gc.pipelines.model._
import fileutils._

class HttpEventSourceTest
    extends FunSuite
    with BeforeAndAfterAll
    with GivenWhenThen
    with ScalatestRouteTest
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
    val runConfigurationFileName = "config-runid"
    val runConfigurationFileContent =
      "demultiplexing=[]"

    val runConfiguration =
      RunConfigurationDTO(runConfigurationFileContent).map(_.toRunConfiguration)
    val runId = "runid"
    val runFolder = new File(watchedFolder, runId)

    Given("a folder watch event source")
    val server = new HttpServer
    val source =
      server.commands

    val probe = TestProbe()
    source.to(Sink.actorRef(probe.ref, "completed")).run()
    When("a run folder is created")
    runFolder.mkdir
    openFileWriter(new File(runFolder, "RunInfo.xml"))(_ => ())
    And("a sample sheet is created")
    openFileWriter(new File(runFolder, runConfigurationFileName)) { writer =>
      writer.write(runConfigurationFileContent)
    }
    Then("the source should not emit")
    probe.expectNoMessage(3 seconds)
    When("the watched file is created")
    val watchedFile = new File(runFolder, fileNameToWatch)
    openFileWriter(watchedFile)(_ => ())
    Then("the source should not emit and the sample sheet should be read")
    probe.expectNoMessage(3 seconds)
    When("a post request is made to /runfolder")
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = Uri("/runfolder"),
      entity = HttpEntity(
        ContentTypes.`application/json`,
        s"""[{"path":"$runFolder","configurationFilePath":"$runFolder/config-runid" }]""")
    )
    request ~> server.route ~> check {
      status shouldEqual StatusCodes.OK
      Then("the source should emit")
      probe.expectMsg(
        application.Append(
          RunfolderReadyForProcessing(RunId(runId),
                                      Some(runFolder.getAbsolutePath),
                                      None,
                                      runConfiguration.right.get)))
      When("the watched file is deleted")
      watchedFile.delete
      Then("the source should not emit")
      probe.expectNoMessage(3 seconds)
    }

  }

}
