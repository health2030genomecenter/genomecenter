package org.gc.pipelines

import tasks._
import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import GenericTestHelpers._

import org.gc.pipelines.application.{
  HttpServer,
  HttpCommandSource,
  FilePipelineState,
  PipelinesApplication,
  ProgressServer
}

import org.gc.pipelines.stages.ProtoPipeline

import org.gc.pipelines.stages._
import GenericTestHelpers._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._

import org.gc.pipelines.application.ProjectFinished
import org.gc.pipelines.model.Project
import tasks.TaskSystemComponents
import akka.stream.Materializer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import akka.util.ByteString
import akka.testkit.TestProbe
import akka.stream.scaladsl.Sink

case class TestApplication[A, B, C](
    pipelinesApplication: PipelinesApplication[A, B, C],
    httpServer: HttpServer
)

object EndToEndHelpers {

  def extractDeliverableList(projectFinished: ProjectFinished[_],
                             project: Project)(
      implicit tsc: TaskSystemComponents,
      mat: Materializer,
      ec: ExecutionContext): List[String] = {
    val deliverableListSF = projectFinished.deliverables.get
      .asInstanceOf[DeliverableList]
      .lists
      .find(_._1 == project)
      .get
      ._2
    scala.io.Source
      .fromString(
        await(
          deliverableListSF.source
            .runFold(ByteString())(_ ++ _)
            .map(_.utf8String)))
      .getLines
      .toList
  }

  def postString(endpoint: String, data: String)(
      implicit app: TestApplication[_, _, _]) = {
    implicit val AS = app.pipelinesApplication.actorSystem
    val binding = await(app.httpServer.startServer)
    await(
      Http().singleRequest(
        HttpRequest(
          uri = s"http://127.0.0.1:${binding.localAddress.getPort}$endpoint",
          method = HttpMethods.POST,
          entity = data)))
  }

  def getProgress(endpoint: String)(implicit app: TestApplication[_, _, _],
                                    ec: ExecutionContext) = {
    implicit val AS = app.pipelinesApplication.actorSystem
    implicit val mat = ActorMaterializer()
    val binding = await(app.httpServer.startServer)
    await(
      Http()
        .singleRequest(
          HttpRequest(
            uri = s"http://127.0.0.1:${binding.localAddress.getPort}$endpoint",
            method = HttpMethods.GET))
        .flatMap(_.entity.toStrict(5 seconds))).data.utf8String
  }

  def createProbe(implicit app: TestApplication[_, _, _]) = {
    implicit val AS = app.pipelinesApplication.actorSystem
    implicit val mat = ActorMaterializer()
    val probe = TestProbe()
    app.pipelinesApplication.processingFinishedSource
      .to(Sink.actorRef(probe.ref, "completed"))
      .run()
    probe
  }

  def withApplication(
      fun: TestApplication[PerSamplePerRunFastQ,
                           SampleResult,
                           DeliverableList] => Unit) = {

    implicit val AS = ActorSystem()
    import AS.dispatcher
    implicit val materializer = ActorMaterializer()
    val (config, basePath) = makeTestConfig
    val commandSource = new HttpCommandSource
    val progressServer = new ProgressServer

    val httpServer =
      new HttpServer(port = 0, Seq(commandSource.route, progressServer.route))
    basePath.mkdirs
    val pipelineState = new FilePipelineState(new File(basePath, "STATE"))
    val taskSystem = defaultTaskSystem(Some(config))

    val pipelineApp = new PipelinesApplication(
      commandSource,
      pipelineState,
      AS,
      taskSystem,
      new ProtoPipeline(progressServer),
      Set.empty)

    val app = TestApplication(pipelineApp, httpServer)

    try {
      fun(app)
    } finally {
      taskSystem.shutdown
    }

  }
}
