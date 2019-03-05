package org.gc.pipelines

import tasks._
import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import GenericTestHelpers._

import org.gc.pipelines.application.{
  HttpServer,
  FilePipelineState,
  PipelinesApplication
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
import akka.util.ByteString
import akka.testkit.TestProbe
import akka.stream.scaladsl.Sink

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
      implicit app: PipelinesApplication[_, _, _]) = {
    implicit val AS = app.actorSystem
    val binding = await(app.eventSource.asInstanceOf[HttpServer].startServer)
    await(
      Http().singleRequest(
        HttpRequest(
          uri = s"http://127.0.0.1:${binding.localAddress.getPort}$endpoint",
          method = HttpMethods.POST,
          entity = data)))
  }

  def createProbe(implicit app: PipelinesApplication[_, _, _]) = {
    implicit val AS = app.actorSystem
    implicit val mat = ActorMaterializer()
    val probe = TestProbe()
    app.processingFinishedSource
      .to(Sink.actorRef(probe.ref, "completed"))
      .run()
    probe
  }

  def withApplication(
      fun: PipelinesApplication[PerSamplePerRunFastQ,
                                SampleResult,
                                DeliverableList] => Unit) = {

    implicit val AS = ActorSystem()
    import AS.dispatcher
    implicit val materializer = ActorMaterializer()
    val (config, basePath) = makeTestConfig
    val eventSource = new HttpServer(port = 0)
    basePath.mkdirs
    val pipelineState = new FilePipelineState(new File(basePath, "STATE"))
    val taskSystem = defaultTaskSystem(Some(config))

    val app = new PipelinesApplication(eventSource,
                                       pipelineState,
                                       AS,
                                       taskSystem,
                                       new ProtoPipeline,
                                       Set.empty)

    try {
      fun(app)
    } finally {
      taskSystem.shutdown
    }

  }
}
