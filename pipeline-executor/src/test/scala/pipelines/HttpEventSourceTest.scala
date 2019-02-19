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
import org.gc.pipelines.util.StableSet
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

  test("v2/register should accept the configuration with runfolder") {
    implicit val materializer = ActorMaterializer()
    val watchedFolder = {
      val tmpFile = TempFile.createTempFile("test")
      tmpFile.delete
      tmpFile.mkdir
      tmpFile
    }

    val runId = "runid"
    val runConfigurationFileContent =
      s"""demultiplexing=[]
      runFolder="$watchedFolder/$runId""""

    val runFolder = new File(watchedFolder, runId)

    Given("a folder watch event source")
    val server = new HttpServer(port = 0)
    val source =
      server.commands

    val probe = TestProbe()
    source.to(Sink.actorRef(probe.ref, "completed")).run()
    When("a run folder is created")
    runFolder.mkdir
    openFileWriter(new File(runFolder, "RunInfo.xml"))(_ => ())
    When("a post request is made to /v2/run/append")
    val request = HttpRequest(method = HttpMethods.POST,
                              uri = Uri("/v2/run/append"),
                              entity = HttpEntity(
                                ContentTypes.`application/json`,
                                runConfigurationFileContent
                              ))
    request ~> server.route ~> check {
      status shouldEqual StatusCodes.OK
      Then("the source should emit")
      probe.expectMsg(
        application.Append(
          RunfolderReadyForProcessing(
            RunId(runId),
            Some(runFolder.getAbsolutePath),
            None,
            RunConfiguration(
              demultiplexingRuns = StableSet(),
              globalIndexSet = None
            )
          )))
    }

  }
  test("v2/register should accept the configuration with fastq files") {
    implicit val materializer = ActorMaterializer()
    val watchedFolder = {
      val tmpFile = TempFile.createTempFile("test")
      tmpFile.delete
      tmpFile.mkdir
      tmpFile
    }

    val fqFile = new File(watchedFolder, "fq1")
    openFileWriter(fqFile)(_ => ())

    val runId = "runid"
    val runConfigurationFileContent =
      s"""demultiplexing=[]
      runId=$runId
      fastqs = [
        {
          project = project 
          sampleId = s1
          lanes = [
            {
              lane = 1
              read1 = $fqFile
              read2 = $fqFile
            }
          ]
        } 
      ]"""

    Given("an http event source")
    val server = new HttpServer(port = 0)
    val source =
      server.commands

    val probe = TestProbe()
    source.to(Sink.actorRef(probe.ref, "completed")).run()

    When("a post request is made to /v2/run/append")
    val request = HttpRequest(method = HttpMethods.POST,
                              uri = Uri("/v2/run/append"),
                              entity = HttpEntity(
                                ContentTypes.`application/json`,
                                runConfigurationFileContent
                              ))
    request ~> server.route ~> check {
      status shouldEqual StatusCodes.OK
      Then("the source should emit")
      probe.expectMsg(
        application.Append(RunfolderReadyForProcessing(
          RunId(runId),
          None,
          Some(
            Seq(
              InputSampleAsFastQ(Set(InputFastQPerLane(Lane(1),
                                                       fqFile.toString,
                                                       fqFile.toString,
                                                       None)),
                                 Project("project"),
                                 SampleId("s1"))
            )),
          RunConfiguration(
            demultiplexingRuns = StableSet(),
            globalIndexSet = None
          )
        )))
    }

  }

  test(
    "HttpServer should emit an event when a post request to /runfolder is made") {
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
      RunConfiguration(runConfigurationFileContent)
    val runId = "runid"
    val runFolder = new File(watchedFolder, runId)

    Given("a folder watch event source")
    val server = new HttpServer(port = 0)
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
