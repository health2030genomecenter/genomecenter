package org.gc.pipelines.cli

import scopt.OParser
import scala.util._
import com.typesafe.config.ConfigFactory
import org.gc.pipelines.application.{
  RunfolderReadyForProcessing,
  AnalysisConfiguration
}
import scalaj.http.Http

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

      # optional, if missing variant calls are made 
      # variantCalls = 
      
      # optional, if missing joint calls are NOT made
      # jointCalls = 

      # optional minimum coverage (WGS, or targeted)
      # If present the pipeline will stop early if coverage is not met 
      # minimumWGSCoverage =
      # minimumTargetCoverage =

      # optional, if missing main human chromosomes ([chr]1-22,X,Y,M,MT) are used
      # path to file 
      # variantCallingContigs =
    }

    # Specify either the wes or the rna object, but not both!
    rna = {
      analysisId = 
      referenceFasta =
      geneModelGtf = 
      qtlToolsCommandLineArguments = # array of strings e.g. [--minimum-mapq,5]
      quantificationGtf = 
    }
  }
  """
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
  case object Assign extends CliCommand
  case object Unassign extends CliCommand
  case object QueryBam extends CliCommand
  case object QueryVcf extends CliCommand
  case object QueryRuns extends CliCommand
  case object QueryProjects extends CliCommand
  case object QueryRunConfigurations extends CliCommand
  case object QueryAnalyses extends CliCommand

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
    else org.gc.pipelines.MainConfig.port

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
      project: Option[String] = None,
      projectFrom: Option[String] = None,
      analysisId: Option[String] = None
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
        println(s"File contents follows: ~~~\n\n $conf \n\n~~~")
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
          "Adds a new run. The run will be processed immediately and on all future restarts of the pipeline (subject to checkpointing). To overwrite the configuration of a run call this command multiple times.")
        .action((_, c) => c.copy(command = AppendRun))
        .children(
          arg[String]("configuration-file")
            .text("path to run configuration, stdin for stdin")
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
      cmd("delete-run")
        .text(
          "Deletes an existing run. A deleted run won't get processed after restarting the pipeline. No files are deleted.")
        .action((_, c) => c.copy(command = DeleteRun))
        .children(
          arg[String]("runID")
            .text("run ID of the run to delete")
            .action((v, c) => c.copy(runId = Some(v)))
            .required
        ),
      cmd("assign-analysis")
        .text(
          "Assigns an analysis configuration to a project name. All samples in the project will be processed with that analysis. You can assign multiple analyses per project, or overwrite existing configuration by calling this command multiple times. You can either specify a new analysis configuration or copy from an existing project.")
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
          "Unassign project - analysisID pair. Samples of the project won't be processed with that analysis in the future. Does not affect already started analyses.")
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
      cmd("query-projects")
        .text("Queries projects or sample status per project")
        .action((_, c) => c.copy(command = QueryProjects))
        .children(
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
            .action((v, c) => c.copy(project = Some(v)))
        )
    )
  }

  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>
      config.command match {
        case QueryAnalyses =>
          (config.project, config.analysisId) match {
            case (None, _) =>
              println(get("/v2/analyses"))
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
        case QueryVcf =>
          val project = config.project.get
          println(get(s"/v2/vcfs/$project"))
        case QueryRuns =>
          config.runId match {
            case None =>
              println(get("/v2/runs"))
            case Some(r) =>
              println(get(s"/v2/runs/$r"))
          }
        case QueryProjects =>
          config.project match {
            case None =>
              println(get("/v2/projects"))
            case Some(project) =>
              println(get(s"/v2/projects/$project"))
          }
        case PrintHelp =>
          println(OParser.usage(parser1))
        case Unassign =>
          println(
            s"Command: unassigned project ${config.project.get} from analysis ${config.analysisId.get}")
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

          val maybeParsed = AnalysisConfiguration.fromConfig(
            ConfigFactory.parseString(configuration))

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
