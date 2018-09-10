package org.gc.slurmsupport

import java.net.InetSocketAddress
import scala.util._
import scala.sys.process._

import tasks.elastic._
import tasks.shared._
import tasks.util._
import tasks.util.config._

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.StrictLogging

object SlurmShutdown extends ShutdownNode {

  def shutdownRunningNode(nodeName: RunningJobId): Unit = {
    execGetStreamsAndCode(s"scancel ${nodeName.value}")
  }

  def shutdownPendingNode(nodeName: PendingJobId): Unit = {
    execGetStreamsAndCode(s"scancel ${nodeName.value}")
  }

}

class SlurmCreateNode(masterAddress: InetSocketAddress,
                      codeAddress: CodeAddress)(
    implicit config: TasksConfig,
    elasticSupport: ElasticSupportFqcn)
    extends CreateNode
    with StrictLogging {

  val additionalSbatchArguments =
    config.raw.getStringList("tasks.slurm.sbatchExtraArguments").asScala

  def requestOneNewJobFromJobScheduler(requestSize: CPUMemoryRequest)
    : Try[Tuple2[PendingJobId, CPUMemoryAvailable]] = {
    val script = Deployment.script(
      memory = requestSize.memory,
      elasticSupport = elasticSupport,
      masterAddress = masterAddress,
      download = new java.net.URL("http",
                                  codeAddress.address.getHostName,
                                  codeAddress.address.getPort,
                                  "/"),
      slaveHostname = None,
      background = false
    )

    val _ = script

    val sbatchCommand = Seq(
      "sbatch",
      s"--wrap=$script",
      s"--mem=${requestSize.memory}",
      s"--mincpus=${requestSize.cpu._1}",
      "--error=slurm-%j.err",
      "--output=slurm-%j.out") ++ additionalSbatchArguments

    logger.debug(s"""Submitting batch job with ${sbatchCommand
      .mkString("[", ", ", "]")} """)

    val (stdout, stderr, _) = execGetStreamsAndCode(
      Process(sbatchCommand)
    )

    logger.debug("Sbatch stdout: " + stdout)
    logger.debug("Sbatch stderr: " + stderr)

    val prefix = "Submitted batch job"
    val pid = stdout.mkString("").drop(prefix.size).trim.toInt

    Try(
      (PendingJobId(pid.toString),
       CPUMemoryAvailable(cpu = requestSize.cpu._1,
                          memory = requestSize.memory)))

  }

}

class SlurmCreateNodeFactory(implicit config: TasksConfig,
                             fqcn: ElasticSupportFqcn)
    extends CreateNodeFactory {
  def apply(master: InetSocketAddress, codeAddress: CodeAddress) =
    new SlurmCreateNode(master, codeAddress)
}

object SlurmGetNodeName extends GetNodeName {
  def getNodeName = {
    val jobid = System.getenv("SLURM_JOB_ID")
    jobid
  }
}

object SlurmSupport extends ElasticSupportFromConfig {
  implicit val fqcn = ElasticSupportFqcn("org.gc.slurmsupport.SlurmSupport")
  def apply(implicit config: TasksConfig) = SimpleElasticSupport(
    fqcn = fqcn,
    hostConfig = None,
    reaperFactory = None,
    shutdown = SlurmShutdown,
    createNodeFactory = new SlurmCreateNodeFactory,
    getNodeName = SlurmGetNodeName
  )
}
