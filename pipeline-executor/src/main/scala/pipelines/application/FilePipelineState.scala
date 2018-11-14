// package org.gc.pipelines.application

// import scala.concurrent.Future
// import com.typesafe.scalalogging.StrictLogging
// import java.io.File
// import io.circe.{Encoder, Decoder}
// import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

// class FilePipelineState(logFile: File)
//     extends PipelineState
//     with StrictLogging {
//   private var incomplete = List[RunfolderReadyForProcessing]()
//   private var completed = List[RunfolderReadyForProcessing]()
//   sealed trait Event
//   case class Registered(run: RunfolderReadyForProcessing) extends Event
//   case class Completed(run: RunfolderReadyForProcessing) extends Event

//   object Event {
//     implicit val encoder: Encoder[Event] =
//       deriveEncoder[Event]
//     implicit val decoder: Decoder[Event] =
//       deriveDecoder[Event]
//   }

//   private def recover() = {
//     fileutils.openSource(logFile)(_.getLines.foreach { line =>
//       updateState(io.circe.parser.decode[Event](line).right.get)
//     })

//     logger.info(s"Recovery completed. Incomplete runs: $incomplete")
//   }

//   val writer = new java.io.FileWriter(logFile, true) // append = true

//   private def appendEvent(e: Event) = synchronized {
//     import io.circe.syntax._
//     val data = e.asJson.noSpaces
//     writer.write(data)
//     writer.write("\n")
//     writer.flush
//   }

//   recover()

//   def updateState(e: Event) = e match {
//     case Registered(r) =>
//       incomplete = r :: incomplete
//     case Completed(r) =>
//       incomplete = incomplete.filterNot(_ == r)
//       completed = r :: completed
//   }

//   def incompleteRuns = {
//     logger.debug(s"Querying incomplete runs (${incomplete.size})")
//     Future.successful(incomplete)
//   }
//   def registerNewRun(r: RunfolderReadyForProcessing) = synchronized {
//     logger.info(s"Registering run ${r.runId}")
//     val event = Registered(r)
//     appendEvent(event)
//     updateState(event)
//     Future.successful(())
//   }
//   def processingFinished(r: RunfolderReadyForProcessing) = synchronized {
//     logger.info(s"Saving finished run ${r.runId}")
//     val event = Completed(r)
//     appendEvent(event)
//     updateState(event)
//     Future.successful(())
//   }
//   def completed(r: RunfolderReadyForProcessing) = {
//     logger.debug(s"Querying run's ${r.runId} completion")
//     Future.successful(completed.contains(r))
//   }
// }
