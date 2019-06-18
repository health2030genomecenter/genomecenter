package org.gc.pipelines.cli

import scopt.OParser
import scala.util._
import com.typesafe.config.ConfigFactory
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  AnalysisConfiguration,
  ProgressData,
  ProgressDataWithSampleId
}
import ProgressData._
import java.io.File
import org.gc.pipelines.model.{
  Project,
  SampleId,
  AnalysisId,
  RunId,
  DemultiplexingSummary
}
import org.gc.pipelines.stages.Demultiplexing
import scalaj.http.Http
import org.gc.pipelines.model.SampleSheet
import org.gc.pipelines.util.BAM

object CliHelpers {
  val runConfigurationExample =
    s"""
  # list of demultiplexing configurations
  # not needed if fastqs are given (see below)
  demultiplexing=[
    {
      # path to sample sheet
      sampleSheet = 
      # some unique id of this demultiplexing configuration
      id =

      # mapping array between read number on the cluster and read number in the pair 
      # [1,2] means the first read is the first in the pair and the second is second in the pair 
      # [1,3] means the first read is the first in the pair and the third read is
      #   the second in the pair 
      readAssignment = [1,2]

      # optional settings
      #
      # umiReadNumber = # integer, usually 2 and readAssignment = [1,3]
      #
      # extraBcl2FastqArguments = # array of strings e.g. [--with-failed-reads]
      # 
      # tenX = # boolean
      # partitionByLane = # boolean
      # partitionByTileCount = # integer
    }
  ]
  
  # Specify either an illumina runfolder or a list of fastqs along with some presumed run id
  # path to illumina run folder
  runFolder=
  
  # if you specify fastq files you still have to give a run id, it does not have to be an illumina run id 
  runId=runId
  fastqs = [
    {
      project = project 
      sampleId = s1
      lanes = [
        {
          lane = 1 # integer
          read1 = /path/to/fq1
          read2 = /path/to/fq2
        }
      ]
    } 
  ]"""
  val analysisConfigurationExample =
    """
  {
    # Specify either the wes or the rna object, but not both!
    wes = {
      analysisId =
      referenceFasta =
      targetIntervals =
      bqsr.knownSites = # array of strings 
      dbSnpVcf = 
      variantEvaluationIntervals = 
      
      # optional vqsr configuration. if missing, vqsr is skipped
      # vqsrMillsAnd1Kg =
      # vqsrHapmap = 
      # vqsrOneKgOmni =
      # vqsrOneKgHighConfidenceSnps =
      # vqsrDbSnp138 = 

      # optional, if missing or true variant calls are made 
      # if present and false variant calls are not made
      # variantCalls = 

      # optional, if missing or false variant calls are deleted after QC
      # if present and true variant calls are kept
      # if keepVcf = true and variantCalls = false, then variant calls are not made
      # keepVcf = 
      
      # optional, if missing or false joint calls are NOT made
      # if present and true joint calls are made
      # if variant calls are not made, or are not kept then joint calls are not made
      # jointCalls = 

      # optional, if missing merged calls are NOT made
      # mergeSingleCalls = 

      # optional minimum coverage (WGS)
      # If present the pipeline will stop early if coverage is not met 
      # minimumWGSCoverage =

      # optional minimum targeted coverage
      # If present the pipeline will stop early if targeted coverage is not met 
      # minimumTargetCoverage =

      # optional, if missing main human chromosomes ([chr]1-22,X,Y,M,MT) are used
      # path to file 
      # variantCallingContigs =

      # optional, if present and true vqsr is tried on single samples
      # singleSampleVqsr =
    }

    # Specify either the wes or the rna object, but not both!
    rna = {
      analysisId = 
      referenceFasta =
      geneModelGtf = 
      qtlToolsCommandLineArguments = # array of strings e.g. [--minimum-mapq,5]
      quantificationGtf = 

      # optional if missing or not exactly 2.6.1c then 2.6.0a is used
      # starVersion = 
    }
  }
  """

  def formatProgressEvents(events: Seq[ProgressData],
                           suppressCompletedTasks: Boolean) = {
    val bySamples = events
      .collect {
        case v: ProgressDataWithSampleId =>
          v
      }
      .groupBy(v => (v.project, v.sample))
    val asString = bySamples
      .flatMap {
        case ((project, sample), events) =>
          case class Status(
              demultiplexed: Seq[RunId] = Nil,
              processing: Seq[RunId] = Nil,
              coverage: Seq[String] = Nil,
              bam: Seq[String] = Nil,
              vcf: Seq[String] = Nil,
              failed: Seq[RunId] = Nil
          ) {
            def waitingForCoverage =
              processing.map(_.toString).toSet &~ coverage.toSet
            def waitingForVCF =
              processing.map(_.toString).toSet &~ vcf.toSet
            def waitingForBam =
              processing.map(_.toString).toSet &~ bam.toSet
          }
          val folded: Status = events.foldLeft(Status()) {
            case (status, event) =>
              event match {
                case ev: DemultiplexedSample =>
                  status.copy(demultiplexed = status.demultiplexed :+ ev.run)
                case ev: SampleProcessingStarted =>
                  status.copy(processing = status.processing :+ ev.run)
                case ev: SampleProcessingFinished =>
                  status.copy(
                    processing = status.processing.filterNot(_ == ev.run))
                case ev: SampleProcessingFailed =>
                  status.copy(processing =
                                status.processing.filterNot(_ == ev.run),
                              failed = status.failed :+ ev.run)
                case ev: FastCoverageAvailable =>
                  status.copy(coverage = status.coverage :+ ev.runIdTag)
                case ev: BamAvailable =>
                  status.copy(bam = status.bam :+ ev.runIdTag)
                case ev: VCFAvailable =>
                  status.copy(vcf = status.vcf :+ ev.runIdTag)
              }
          }

          (folded.demultiplexed.map { run =>
            if (suppressCompletedTasks) Nil
            else List(project, sample, "DEMULTIPLEX", run)
          } ++
            folded.processing.map { run =>
              List(project, sample, "PROCESSING ", run)
            } ++
            folded.coverage.map { run =>
              if (suppressCompletedTasks) Nil
              else List(project, sample, "COV DONE   ", run)
            } ++
            folded.waitingForCoverage.map { run =>
              List(project, sample, "COV WAIT   ", run)
            } ++
            folded.bam.map { run =>
              if (suppressCompletedTasks) Nil
              else List(project, sample, "BAM DONE   ", run)
            } ++
            folded.waitingForBam.map { run =>
              List(project, sample, "BAM WAIT   ", run)
            } ++
            folded.vcf.map { run =>
              if (suppressCompletedTasks) Nil
              else List(project, sample, "VCF DONE   ", run)
            } ++
            folded.waitingForVCF.map { run =>
              List(project, sample, "VCF WAIT   ", run)
            } ++
            folded.failed.map { run =>
              List(project, sample, "FAILED     ", run)
            }).map(_.mkString("\t"))
      }
      .filter(_.nonEmpty)
      .mkString("", "\n", "\n")

    case class JointCallState(started: Seq[JointCallsStarted] = Nil,
                              done: Seq[JointCallsAvailable] = Nil)
    val jointCallFolded = events.foldLeft((JointCallState())) {
      case (folded, event) =>
        event match {
          case j: JointCallsStarted =>
            folded.copy(started = folded.started :+ j)
          case j: JointCallsAvailable =>
            folded.copy(
              started = folded.started.filterNot(v =>
                v.analysisId == j.analysisId && v.project == j.project),
              done = folded.done :+ j)
          case _ => folded
        }
    }

    val jointCallAsString = (jointCallFolded.started.map { started =>
      List(started.project,
           started.analysisId,
           started.samples.size,
           "JOINTCALL RUNNING",
           started.runs.toSeq.sortBy(_.toString).mkString(","))
    } ++ jointCallFolded.done.map { done =>
      if (suppressCompletedTasks) Nil
      else
        List(done.project,
             done.analysisId,
             done.samples.size,
             "JOINTCALL DONE   ",
             done.runs.toSeq.sortBy(_.toString).mkString(","))
    }).map(_.mkString("\t")).mkString("", "\n", "\n")

    asString + "\n" + jointCallAsString
  }
}

/* Command line interface
 *
 * - append run
 * - delete run
 * - assign analysis
 * - unassign analysis
 * - query bam by project
 * - query vcf by project
 * - query runs
 * - query projects
 *
 */
object Pipelinectl extends App {

  sealed trait CliCommand
  case object PrintHelp extends CliCommand
  case object AppendRun extends CliCommand
  case object DeleteRun extends CliCommand
  case object DeleteFreeRuns extends CliCommand
  case object SendReprocessAllRuns extends CliCommand
  case object Assign extends CliCommand
  case object Unassign extends CliCommand
  case object QueryBam extends CliCommand
  case object QueryVcf extends CliCommand
  case object QueryFastq extends CliCommand
  case object QueryCoverage extends CliCommand
  case object QueryRuns extends CliCommand
  case object QueryProjects extends CliCommand
  case object QueryRunConfigurations extends CliCommand
  case object QueryAnalyses extends CliCommand
  case object QueryDeliverables extends CliCommand
  case object QueryFreeRuns extends CliCommand
  case object AnalyseResourceUsage extends CliCommand
  case object ParseSampleSheet extends CliCommand
  case object QuerySampleIndices extends CliCommand

  val config = {
    val configInUserHome =
      new java.io.File(System.getProperty("user.home") + "/.gc/config")
    if (configInUserHome.canRead)
      ConfigFactory
        .parseFile(configInUserHome)
        .withFallback(ConfigFactory.load())
    else ConfigFactory.load()
  }
  val hostname = config.getString("gc.cli.hostname")
  val port =
    if (config.hasPath("gc.cli.port")) config.getInt("gc.cli.port")
    else config.getInt("gc.http.port")

  def post(endpoint: String, data: String) = {
    Http(s"http://$hostname:$port$endpoint")
      .postData(data)
      .header("content-type", "application/json")
      .asString
  }
  def post(endpoint: String) = {
    Http(s"http://$hostname:$port$endpoint")
      .method("POST")
      .asString
  }
  def get(endpoint: String) =
    Http(s"http://$hostname:$port$endpoint")
      .method("GET")
      .asString
      .body

  def delete(endpoint: String) =
    Http(s"http://$hostname:$port$endpoint")
      .method("DELETE")
      .asString

  case class Config(
      command: CliCommand = PrintHelp,
      configPath: Option[String] = None,
      runId: Option[String] = None,
      runIds: Seq[String] = Nil,
      project: Option[String] = None,
      projectFrom: Option[String] = None,
      analysisId: Option[String] = None,
      sampleId: Option[String] = None,
      subtree: Option[String] = None,
      printDot: Option[Boolean] = None,
      samplesFile: Option[String] = None,
      listProjects: Option[Boolean] = None,
      queryProgressOfAllProjects: Boolean = false,
      suppressCompletedTasks: Boolean = false,
      queryFastQsOfFreeRuns: Boolean = false,
      outFile: Option[String] = None
  )

  def printAddRunHelpAndExit() = {
    println(CliHelpers.runConfigurationExample)
    System.exit(0)
  }

  def printAssignHelpAndExit() = {
    println(CliHelpers.analysisConfigurationExample)
    System.exit(0)
  }

  def readFileOrStdin(path: String) =
    path match {
      case "stdin" =>
        println("  Waiting for input on stdin..")
        scala.io.Source.stdin.mkString
      case path =>
        val conf = fileutils.openSource(path)(_.mkString)
        println(s"File contents follows: ~~~\n\n$conf \n\n~~~")
        println("Type Y if ok!")
        if (scala.io.Source.stdin.take(1).mkString != "Y") {
          System.exit(0)
          ???
        } else conf
    }

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("pipelinectl"),
      head("Command line tool to control the pipeline. Version:",
           org.gc.buildinfo.BuildInfo.version),
      help("help"),
      version("version"),
      cmd("add-run")
        .text(
          "Adds a new run. The run will be processed immediately and on all future restarts of the pipeline. To overwrite the configuration of a run invoke this command multiple times.")
        .action((_, c) => c.copy(command = AppendRun))
        .children(
          arg[String]("configuration-file")
            .text("path to run configuration, stdin for stdin. The configuration is copied and the file is not referenced in the future.")
            .action((v, c) => c.copy(configPath = Some(v)))
            .validate(v =>
              if (v == "stdin") success
              else if (new java.io.File(v).canRead) success
              else failure("can't read."))
            .required,
          cmd("template")
            .action { (_, c) =>
              printAddRunHelpAndExit()
              c
            }
            .text("print template configuration for add-run and exit")
        ),
      cmd("parse-samplesheet")
        .text("Parse sample sheet, print it and exit")
        .action((_, c) => c.copy(command = ParseSampleSheet))
        .children(
          arg[String]("sampleSheet")
            .text("path to sample sheet file")
            .action((v, c) => c.copy(configPath = Some(v)))
            .required
        ),
      cmd("reprocess-all")
        .text("Reprocesses all runs")
        .action((_, c) => c.copy(command = SendReprocessAllRuns)),
      cmd("delete-run")
        .text(
          "Deletes an existing run. A deleted run won't get processed after restarting the pipeline. No files are deleted from the disk. The run's reads won't show up in any future analyses and reports. Does not affect currently running jobs (i.e. does not stop jobs).")
        .action((_, c) => c.copy(command = DeleteRun))
        .children(
          arg[String]("runID")
            .text("run ID of the run to delete")
            .action((v, c) => c.copy(runId = Some(v)))
            .required
        ),
      cmd("delete-freeruns")
        .text(
          "Deletes all existing runs. A deleted run won't get processed after restarting the pipeline. No files are deleted from the disk. The run's reads won't show up in any future analyses and reports. Does not affect currently running jobs (i.e. does not stop jobs).")
        .action((_, c) => c.copy(command = DeleteFreeRuns))
        .children(
          opt[Seq[String]]('k', "keep")
            .text("run ID of the run to NOT delete")
            .action((v, c) => c.copy(runIds = v))
        ),
      cmd("assign-analysis")
        .text(
          "Assigns an analysis configuration to a project name. All samples in the project will be processed with that analysis. You can assign multiple analyses per project, or overwrite existing configuration by calling this command multiple times. You can either specify a new analysis configuration or copy from an existing project. The configuration is copied and the file is not referenced any more. ")
        .action((_, c) => c.copy(command = Assign))
        .children(
          opt[String]('p', "project")
            .text("project name to assign to")
            .action((v, c) => c.copy(project = Some(v)))
            .required,
          opt[String]('c', "conf")
            .text("path to configuration of analysis. stdin for stdin")
            .action((v, c) => c.copy(configPath = Some(v)))
            .validate(v =>
              if (v == "stdin") success
              else if (new java.io.File(v).canRead) success
              else failure("can't read.")),
          opt[String]("from-project")
            .text("project name from which we copy")
            .action((v, c) => c.copy(projectFrom = Some(v))),
          opt[String]("from-analysis")
            .text("analysis id from which we copy")
            .action((v, c) => c.copy(analysisId = Some(v))),
          cmd("template")
            .action { (_, c) =>
              printAssignHelpAndExit()
              c
            }
            .text("print template configuration for assign-analysis and exit"),
        ),
      cmd("unassign-analysis")
        .text(
          "Unassign project - analysisID pair. Samples of the project won't be processed with that analysis in the future. Does not affect already started analyses (does not stop jobs). Future analyses and reports won't show up the deleted analysis.")
        .action((_, c) => c.copy(command = Unassign))
        .children(
          opt[String]('p', "project")
            .text("project name to unassign")
            .action((v, c) => c.copy(project = Some(v)))
            .required,
          opt[String]('a', "analysis")
            .text("analysis ID to unassign")
            .action((v, c) => c.copy(analysisId = Some(v)))
            .required
        ),
      cmd("query-bams")
        .text("Queries finished bam files per project")
        .action((_, c) => c.copy(command = QueryBam))
        .children(
          arg[String]("project")
            .text("project name ")
            .action((v, c) => c.copy(project = Some(v)))
            .required
        ),
      cmd("query-vcfs")
        .text("Queries finished vcf files per project")
        .action((_, c) => c.copy(command = QueryVcf))
        .children(
          arg[String]("project")
            .text("project name ")
            .action((v, c) => c.copy(project = Some(v)))
            .required
        ),
      cmd("query-deliverables")
        .text("Queries deliverables per project")
        .action((_, c) => c.copy(command = QueryDeliverables))
        .children(
          arg[String]("project")
            .text("project name ")
            .action((v, c) => c.copy(project = Some(v)))
            .required
        ),
      cmd("query-fastqs")
        .text("Queries finished fastq files per project")
        .action((_, c) => c.copy(command = QueryFastq))
        .children(
          arg[String]("project")
            .text("project name ")
            .action((v, c) => c.copy(project = Some(v)))
            .required
        ),
      cmd("query-projects")
        .text(
          "Queries projects or sample status per project. Shows status messages for each run: \n DEMULTIPLEX - run is demultiplexed\nPROCESSING - reads from this run belonging to this sample are being processed\nCOV DONE - coverage done\nCOV WAIT - waiting for coverage data from this run for this sample\nBAM DONE - \nBAM WAIT - \nVCF DONE - \nVCF WAIT - \n")
        .action((_, c) => c.copy(command = QueryProjects))
        .children(
          opt[Unit]("all")
            .text("show progress of all projects")
            .action((_, c) => c.copy(queryProgressOfAllProjects = true)),
          opt[Unit]("suppress-done")
            .text("do not show completed tasks")
            .action((_, c) => c.copy(suppressCompletedTasks = true)),
          opt[String]('p', "project")
            .text("project name. If missing lists all projects. If present lists sample status per project.")
            .action((v, c) => c.copy(project = Some(v)))
        ),
      cmd("query-runs")
        .text("Queries runs or progress of runs")
        .action((_, c) => c.copy(command = QueryRuns))
        .children(
          opt[String]('r', "run")
            .text("run id. If given shows progress of run. Otherwise lists all runs.")
            .action((v, c) => c.copy(runId = Some(v)))
        ),
      cmd("query-coverages")
        .text("Queries coverages of project")
        .action((_, c) => c.copy(command = QueryCoverage))
        .children(
          arg[String]("project")
            .text("project name.")
            .action((v, c) => c.copy(project = Some(v)))
            .required
        ),
      cmd("query-runconfig")
        .text("Query run configurations")
        .action((_, c) => c.copy(command = QueryRunConfigurations))
        .children(
          opt[String]('r', "run")
            .text("run ID of the run. Lists all configurations if missing.")
            .action((v, c) => c.copy(runId = Some(v)))
        ),
      cmd("query-analyses")
        .text("Query analysis configurations")
        .action((_, c) => c.copy(command = QueryAnalyses))
        .children(
          opt[String]('p', "project")
            .text("project name. If missing lists all.")
            .action((v, c) => c.copy(project = Some(v))),
          opt[String]('a', "analysisID")
            .text("analysis id. If missing lists all. Only relevant if project is given as well.")
            .action((v, c) => c.copy(analysisId = Some(v))),
          opt[Unit]("list-projects")
            .text("List all projects with active analysis configuration")
            .action((_, c) => c.copy(listProjects = Some(true)))
        ),
      cmd("query-freeruns")
        .text("Query runs which contain unprocessed projects")
        .action((_, c) => c.copy(command = QueryFreeRuns))
        .children(
          opt[Unit]("fastqs")
            .text("List fastq files of free runs. (SLOW)")
            .action((_, c) => c.copy(queryFastQsOfFreeRuns = true))
        ),
      cmd("query-sample-indices")
        .text("Extract index data from demultiplexing reports")
        .action((_, c) => c.copy(command = QuerySampleIndices))
        .children(
          opt[String]('p', "project")
            .text("project name. If missing lists all.")
            .action((v, c) => c.copy(project = Some(v)))
        ),
      cmd("analyse-resource-usage")
        .text("Analyses resource usage log")
        .action((_, c) => c.copy(command = AnalyseResourceUsage))
        .children(
          opt[String]('p', "project")
            .text("project name")
            .action((v, c) => c.copy(project = Some(v)))
            .required,
          opt[String]('s', "sample")
            .text("sample id, if missing all samples of the project are used")
            .action((v, c) => c.copy(sampleId = Some(v))),
          opt[String]('a', "analysis")
            .text("analysis id, if missing all analyses are used")
            .action((v, c) => c.copy(analysisId = Some(v))),
          opt[String]('r', "run")
            .text("run id, if missing all runs are used")
            .action((v, c) => c.copy(runId = Some(v))),
          opt[String]('f', "file")
            .text("log file. This file is written by the pipeline daemon to the path specified in the 'tasks.tracker.logFile' configuration key.")
            .action((v, c) => c.copy(configPath = Some(v)))
            .required,
          opt[String]("subtree")
            .text("ID of the root of subtree, if set will analyze a subtree")
            .action((v, c) => c.copy(subtree = Some(v))),
          opt[Unit]("dot")
            .text("print dot document for graphviz, if missing summary statistics are printed")
            .action((_, c) => c.copy(printDot = Some(true))),
          opt[String]("out-file")
            .text(
              "optional file path to write selected nodes. Used for debugging.")
            .action((v, c) => c.copy(outFile = Some(v))),
        )
    )
  }

  def demultiplexedReadNumberOfRunBySample(runId: String) = {
    val runEvents = io.circe.parser
      .decode[Seq[ProgressData]](get(s"/v2/runs/$runId"))
      .right
      .get
      .collect {
        case v: Demultiplexed => v
      }
      .distinct

    runEvents
      .flatMap {
        case Demultiplexed(_, _, stats) =>
          stats.flatMap {
            case (_, stats) =>
              stats.ConversionResults.flatMap { conversionResult =>
                conversionResult.DemuxResults.map { demuxResultsOfSample =>
                  ((stats.RunId,
                    demuxResultsOfSample.SampleId,
                    conversionResult.LaneNumber),
                   demuxResultsOfSample.NumberReads)
                }
              }
          }
      }
  }

  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>
      config.command match {
        case DeleteFreeRuns =>
          val runs = io.circe.parser
            .decode[List[(RunId, Seq[String])]](get("/v2/free-runs"))
            .right
            .get
            .map(_._1)
            .distinct

          val keep = config.runIds.toSet

          val runsToDelete = runs.filterNot(keep)

          println(
            s"Deleting runs: \n${runsToDelete.mkString("\n")} \n Type Y if OK")
          if (scala.io.Source.stdin.take(1).mkString != "Y") {
            System.exit(0)
            ???
          }
          runsToDelete.foreach { runId =>
            val response = delete("/v2/runs/" + runId)
            if (response.code != 200) {
              println(s"$runId Request failed: " + response)
            } else {
              println(s"$runId OK")
            }
          }

        case QueryFreeRuns =>
          val response =
            if (config.queryFastQsOfFreeRuns) get("/v2/free-runs?fastq=true")
            else get("/v2/free-runs")

          val parsed = io.circe.parser
            .decode[List[(RunId, Seq[String])]](response)
            .right
            .get

          if (config.queryFastQsOfFreeRuns)
            parsed.foreach {
              case (_, fastqs) =>
                fastqs.foreach { path =>
                  println(path)
                }
            } else parsed.foreach { case (run, _) => println(run) }
        case SendReprocessAllRuns =>
          val response = post("/v2/reprocess")
          if (response.code != 200) {
            println("Request failed: " + response)
          } else {
            println("OK")
          }
        case AnalyseResourceUsage =>
          ResourceUsage.plotSample(
            new File(config.configPath.get),
            Project(config.project.get),
            config.sampleId.map(s => SampleId(s)),
            config.analysisId.map(s => AnalysisId(s)),
            config.runId.map(s => RunId(s)),
            config.subtree,
            config.printDot.getOrElse(false),
            config.outFile
          )
        case QueryAnalyses =>
          (config.project, config.analysisId) match {
            case (None, _) =>
              if (config.listProjects.exists(identity)) {
                println(get("/v2/analysed-projects"))
              } else println(get("/v2/analyses"))

            case (Some(project), None) =>
              println(get(s"/v2/analyses/$project"))
            case (Some(project), Some(analysisId)) =>
              println(get(s"/v2/analyses/$project/$analysisId"))
          }
        case QueryRunConfigurations =>
          config.runId match {
            case None =>
              println(get("/v2/runconfigurations"))
            case Some(runId) =>
              println(get(s"/v2/runconfigurations/$runId"))
          }
        case QueryBam =>
          val project = config.project.get
          println(get(s"/v2/bams/$project"))
        case QueryDeliverables =>
          val project = config.project.get
          val deliveryList = io.circe.parser
            .decode[Option[ProgressData]](get(s"/v2/deliveries/$project"))
            .right
            .get
            .collect {
              case d: DeliveryListAvailable => d
            }
          deliveryList match {
            case None => println("Nothing to deliver yet.")
            case Some(deliveryList) =>
              println("Runs included:\n")
              deliveryList.runsIncluded.foreach(println)
              println(
                s"\nSamples included (${deliveryList.samples.size} total):\n")
              deliveryList.samples.toSeq.sortBy(_.toString).foreach(println)
              println(s"Files:\n")
              deliveryList.files.foreach(println)

              println("Fetching sample configuration data..")

              val lanesBySample = {
                val runConfigurations = io.circe.parser
                  .decode[Seq[RunfolderReadyForProcessing]](
                    get(s"/v2/runconfigurations"))
                  .right
                  .get

                val runIds = runConfigurations.map(_.runId)
                val demultiplexingResultsByLaneAndSample = runIds.flatMap {
                  runId =>
                    demultiplexedReadNumberOfRunBySample(runId)
                }.toMap

                val samples = runConfigurations.flatMap { runConfig =>
                  runConfig.sampleSheets.flatMap {
                    case (sampleSheet, _) =>
                      sampleSheet.poolingLayout.flatMap { multiplex =>
                        val demultiplexedReadNumberofSampleInLane =
                          demultiplexingResultsByLaneAndSample.get(
                            (runConfig.runId,
                             multiplex.sampleId,
                             multiplex.lane))

                        val demultiplexedSomeReads =
                          demultiplexedReadNumberofSampleInLane.exists(_ > 0)

                        if (demultiplexedSomeReads)
                          List(
                            (multiplex.project + "." + multiplex.sampleId,
                             runConfig.runId + "." + multiplex.lane))
                        else Nil
                      }
                  }
                }

                samples.groupBy { case (sampleId, _) => sampleId }.map {
                  case (key, group) => (key, group.map(_._2))
                }
              }
              val bamFiles = deliveryList.files.filter(_.endsWith(".bam"))
              println(s"Verifying ${bamFiles.size} bam files..")
              val missingLanesPerFile = bamFiles
                .map { file =>
                  val readGroups = BAM.getReadGroups(new File(file))
                  val platformUnitsInFile =
                    readGroups.map(_.getPlatformUnit).toSet
                  val sampleIds = readGroups.map(_.getSample)
                  assert(sampleIds.distinct.size == 1)
                  val sampleIdInFile = sampleIds.head
                  val expectedPlatformUnits =
                    lanesBySample(sampleIdInFile).toSet
                  val missing = expectedPlatformUnits &~ platformUnitsInFile
                  (file, missing)
                }
                .filter(_._2.nonEmpty)

              if (missingLanesPerFile.isEmpty) {
                println(s"All ${bamFiles.size} contain all expected lanes.")
              } else {
                println(
                  s"Missing lanes (${missingLanesPerFile.size} files):\nFILE\tLANES")
                missingLanesPerFile.foreach {
                  case (file, missingPlatformUnits) =>
                    println(s"$file\t${missingPlatformUnits.mkString(",")}")
                }
              }

          }

        case QueryCoverage =>
          val project = config.project.get
          val coverages = io.circe.parser
            .decode[Seq[ProgressData]](get(s"/v2/coverages/$project"))
            .right
            .get
            .collect {
              case v: FastCoverageAvailable => v
            }
            .distinct

          val perRun = coverages
            .collect {
              case FastCoverageAvailable(project,
                                         sample,
                                         run,
                                         analysis,
                                         coverage) =>
                project + "\t" + sample + "\t" + run + "\t" + analysis + "\t" + coverage
            }
            .sorted
            .distinct
            .mkString("\n")
          val total = coverages
            .groupBy(_.sample)
            .map {
              case (sample, group) =>
                sample + "\t" + group.map(_.wgsCoverage).sum
            }
            .toSeq
            .sorted
            .distinct
            .mkString("\n")

          println("PER RUN")
          println(perRun)
          println("TOTAL")
          println(total)
          println(
            "\nWhat this is: number of mapped, mapQ >20, read length >30, properly paired, pass filter reads over total sequence length (as in the fasta).")
        case QueryFastq =>
          val project = config.project.get
          println(get(s"/v2/fastqs/$project"))
        case QueryVcf =>
          val project = config.project.get
          println(get(s"/v2/vcfs/$project"))
        case QuerySampleIndices =>
          val runIds = scala.io.Source
            .fromString(get("/v2/runs"))
            .getLines
            .toList
            .filter(_.nonEmpty)

          val sampleIndices = runIds
            .flatMap { runId =>
              val runEvents = io.circe.parser
                .decode[Seq[ProgressData]](get(s"/v2/runs/$runId"))
                .right
                .get
                .collect {
                  case v: Demultiplexed => v
                }
                .distinct

              runEvents
                .flatMap {
                  case Demultiplexed(_, samples, stats) =>
                    stats.flatMap {
                      case (_, stats) =>
                        DemultiplexingSummary
                          .fromStats(
                            stats,
                            samples.map {
                              case (project, sample, _) => sample -> project
                            }.toMap,
                            Demultiplexing.readGlobalIndexSetFromClassPath)
                          .sampleIndices
                    }
                }
            }
            .filter { sampleIndex =>
              config.project match {
                case None          => true
                case Some(project) => project == sampleIndex.project
              }
            }

          import io.circe.syntax._
          println(sampleIndices.asJson.noSpaces)

        case QueryRuns =>
          config.runId match {
            case None =>
              println(get("/v2/runs"))
            case Some(runId) =>
              val runEvents = io.circe.parser
                .decode[Seq[ProgressData]](get(s"/v2/runs/$runId"))
                .right
                .get
                .collect {
                  case v: Demultiplexed => v
                }
                .distinct

              val asString = runEvents
                .flatMap {
                  case Demultiplexed(_, samples, stats) =>
                    stats.map {
                      case (demultiplexingId, stats) =>
                        "Demultiplexing run with ID: " + demultiplexingId + ":\n"
                        DemultiplexingSummary.renderAsTable(
                          DemultiplexingSummary.fromStats(
                            stats,
                            samples.map {
                              case (project, sample, _) => sample -> project
                            }.toMap,
                            Demultiplexing.readGlobalIndexSetFromClassPath))
                    }
                }
                .mkString("", "\n", "\n")

              println(asString)

          }
        case QueryProjects =>
          config.project match {
            case None if !config.queryProgressOfAllProjects =>
              println(get("/v2/projects"))
            case None =>
              val events =
                io.circe.parser
                  .decode[Seq[ProgressData]](get(s"/v2/projects?progress=true"))
                  .right
                  .get
              println(
                CliHelpers.formatProgressEvents(
                  events,
                  suppressCompletedTasks = config.suppressCompletedTasks))
            case Some(project) =>
              val projectEvents =
                io.circe.parser
                  .decode[Seq[ProgressData]](get(s"/v2/projects/$project"))
                  .right
                  .get
              println(
                CliHelpers.formatProgressEvents(
                  projectEvents,
                  suppressCompletedTasks = config.suppressCompletedTasks))

          }

        case PrintHelp =>
          println(OParser.usage(parser1))
        case Unassign =>
          println(
            s"Command: unassign project ${config.project.get} from analysis ${config.analysisId.get}")
          val response =
            delete(
              "/v2/analyses/" + config.project.get + "/" + config.analysisId.get)
          if (response.code != 200) {
            println("Request failed: " + response)
          } else {
            println("OK")
          }

        case Assign =>
          println(s"Command: assign project ${config.project.get} to analysis")

          val configuration = config.configPath match {
            case Some(configPath) =>
              println(s"Reading new configuration from file $configPath")
              readFileOrStdin(configPath)
            case None =>
              (config.projectFrom, config.analysisId) match {
                case (Some(projectFrom), Some(analysisId)) =>
                  println(
                    s"Attempt to copy configuration from $projectFrom - $analysisId")
                  get(s"/v2/analyses/$projectFrom/$analysisId")
                case _ =>
                  println(
                    "You have to specify either --conf or both --from-project and --from-analysis")
                  System.exit(1)
                  ???
              }
          }

          val maybeParsed = {
            val parsedFromJson = io.circe.parser
              .decode[AnalysisConfiguration](configuration)

            if (parsedFromJson.isRight) parsedFromJson.left.map(_.toString)
            else
              AnalysisConfiguration.fromConfig(
                ConfigFactory.parseString(configuration))
          }

          maybeParsed match {
            case Left(error) =>
              println(error.toString)
              System.exit(1)
            case Right(configuration)
                if configuration.validationErrors.nonEmpty =>
              configuration.validationErrors.foreach(println)
              System.exit(1)
            case _ =>
              println("Validation passed.")
          }
          val response =
            post("/v2/analyses/" + config.project.get, configuration)
          if (response.code != 200) {
            println("Request failed: " + response)
          } else {
            println("OK")
          }
        case DeleteRun =>
          println("Command: delete run")
          val run = config.runId match {
            case Some(run) => run
            case None =>
              throw new RuntimeException("should not happen")
          }
          val response = delete("/v2/runs/" + run)
          if (response.code != 200) {
            println("Request failed: " + response)
          } else {
            println("OK")
          }

        case ParseSampleSheet =>
          val raw = readFileOrStdin(config.configPath.get)
          println(SampleSheet(raw).parsed.poolingLayout)
        case AppendRun =>
          println("Command: add run")

          val configuration = readFileOrStdin(config.configPath.get)

          val maybeRunFolderReadyEvent = for {
            parsed <- Try(ConfigFactory.parseString(configuration)).toEither.left
              .map(_.toString)
            runFolder <- RunfolderReadyForProcessing.fromConfig(parsed)
          } yield runFolder

          val parsedRunfolder = maybeRunFolderReadyEvent match {
            case Left(error) =>
              println(error.toString)
              System.exit(1)
              ???
            case Right(runFolder) if runFolder.validationErrors.nonEmpty =>
              runFolder.validationErrors.foreach(println)
              System.exit(1)
              ???
            case Right(runFolder) =>
              println("Validation passed.")
              runFolder
          }

          val projects = parsedRunfolder.projects
          println(
            "This run contains the following projects: \n" + projects.toSeq
              .sortBy(_.toString)
              .mkString("\n"))

          val projectsWithoutAnalysis = projects.filter { project =>
            val analyses = get(s"/v2/analyses/$project")
            analyses.isEmpty
          }

          if (projectsWithoutAnalysis.nonEmpty) {
            println(
              s"Projects ${projectsWithoutAnalysis.mkString(", ")} have no analyses assigned to it. You most likely want to assign an analysis first.")
          }

          println(
            "Verify index2 (i5) against known TruSeq unique dual and combinatorial dual indices? Type H for HiSeq4000, N for NovaSeq, anything else to skip this check.")
          scala.io.Source.stdin.take(1).mkString match {
            case "N" =>
              parsedRunfolder.sampleSheets
                .map {
                  case (sampleSheet, sampleSheetFile) =>
                    val potentialWrongI5s = sampleSheet.poolingLayout
                      .filter(_.index2IsKnownTruSeqOnHiSeq4000)
                    (sampleSheet, sampleSheetFile, potentialWrongI5s)
                }
                .filter { case (_, _, wrongs) => wrongs.nonEmpty }
                .foreach {
                  case (_, file, wrongs) =>
                    val num = wrongs.size
                    println(
                      s"$num entries in $file have an I5 index matching the HiSeq4000 form of a TruSeq index.")
                }
            case "H" =>
              parsedRunfolder.sampleSheets
                .map {
                  case (sampleSheet, sampleSheetFile) =>
                    val potentialWrongI5s = sampleSheet.poolingLayout
                      .filter(
                        _.reverseComplementOfIndex2IsKnownTruSeqOnHiSeq4000)
                    (sampleSheet, sampleSheetFile, potentialWrongI5s)
                }
                .filter { case (_, _, wrongs) => wrongs.nonEmpty }
                .foreach {
                  case (_, file, wrongs) =>
                    val num = wrongs.size
                    println(
                      s"$num entries in $file have an I5 index matching the NovaSeq form of a TruSeq index.")
                }
            case _ =>
              println("I5 index check skipped.")
          }

          println(
            "Please confirm to continue with adding the run (type exactly Y)")
          if (scala.io.Source.stdin.take(1).mkString != "Y") {
            System.exit(0)
            ???
          }

          val response = post("/v2/runs", configuration)
          if (response.code != 200) {
            println("Request failed: " + response)
          } else {
            println("OK")
          }
        case command =>
          println(s"command $command not implemened yet")
      }

    case _ =>
    // arguments are bad, error message was displayed
  }
}
