package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fileutils.TempFile
import org.gc.pipelines.util.Exec
import org.gc.pipelines.util
import org.gc.pipelines.model._

case class FastpReport(html: SharedFile,
                       json: SharedFile,
                       project: Project,
                       sampleId: SampleId,
                       runId: RunId,
                       lane: Lane)
    extends WithSharedFiles(html, json)

object Fastp {

  private def extractFastpExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/fastp_v0.19.4_mac"
      else if (util.isLinux) "/bin/fastp_v0.19.4_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "fastp")
      .getAbsolutePath
  }

  val report =
    AsyncTask[FastQPerLaneWithMetadata, FastpReport]("__fastp-report", 1) {
      case FastQPerLaneWithMetadata(
          FastQPerLane(lane, FastQ(read1, _), FastQ(read2, _), _),
          project,
          sampleId,
          runId) =>
        implicit computationEnvironment =>
          val fastpExecutable = extractFastpExecutable()

          for {
            read1 <- read1.file
            read2 <- read2.file
            result <- {

              val tmpHtml = TempFile.createTempFile(".html")
              tmpHtml.delete
              val tmpJson = TempFile.createTempFile(".json")
              tmpJson.delete

              val bashScript = s""" \\
        $fastpExecutable \\
          -i ${read1.getAbsolutePath} \\
          -I ${read2.getAbsolutePath} \\
          -h ${tmpHtml.getAbsolutePath} \\
          -j ${tmpJson.getAbsolutePath} \\
          --dont_overwrite \\
          --report_title '$project: $sampleId $runId-$lane' \\
          --thread ${resourceAllocated.cpu} """

              Exec.bash(logDiscriminator = "fastp",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = project + "." + sampleId + "." + runId + "." + lane

              for {
                json <- SharedFile(tmpJson,
                                   name = nameStub + ".fastp.json",
                                   deleteFile = true)
                html <- SharedFile(tmpHtml,
                                   name = nameStub + ".fastp.html",
                                   deleteFile = true)
              } yield
                FastpReport(html = html,
                            json = json,
                            project,
                            sampleId,
                            runId,
                            lane)
            }
          } yield result

    }
}

object FastpReport {
  implicit val encoder: Encoder[FastpReport] =
    deriveEncoder[FastpReport]
  implicit val decoder: Decoder[FastpReport] =
    deriveDecoder[FastpReport]
}
