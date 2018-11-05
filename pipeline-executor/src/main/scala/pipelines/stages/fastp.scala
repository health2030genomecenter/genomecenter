package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import fileutils.TempFile
import org.gc.pipelines.util.Exec
import org.gc.pipelines.util
import org.gc.pipelines.model._
import scala.concurrent.Future

case class FastpReport(html: SharedFile,
                       json: SharedFile,
                       project: Project,
                       sampleId: SampleId,
                       runId: RunId)
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
    AsyncTask[PerSampleFastQ, FastpReport]("__fastp-report", 1) {
      case PerSampleFastQ(lanes, project, sampleId, runId) =>
        implicit computationEnvironment =>
          val fastpExecutable = extractFastpExecutable()

          def fetchFiles(f: Seq[SharedFile]) = Future.traverse(f)(_.file)

          val lanesSeq = lanes.toSeq
          for {
            read1 <- fetchFiles(lanesSeq.map(_.read1.file))
            read2 <- fetchFiles(lanesSeq.map(_.read2.file))
            result <- {

              val tmpRead1 = TempFile.createTempFile(read1.head.getName)
              val tmpRead2 = TempFile.createTempFile(read2.head.getName)

              {
                import better.files._
                tmpRead1.toScala.newOutputStream.autoClosed.foreach { out =>
                  read1.foreach { f =>
                    for {
                      in <- f.toScala.newInputStream.autoClosed
                    } in.pipeTo(out)
                  }
                }
                tmpRead2.toScala.newOutputStream.autoClosed.foreach { out =>
                  read2.foreach { f =>
                    for {
                      in <- f.toScala.newInputStream.autoClosed
                    } in.pipeTo(out)
                  }
                }
              }
              val tmpHtml = TempFile.createTempFile(".html")
              tmpHtml.delete
              val tmpJson = TempFile.createTempFile(".json")
              tmpJson.delete

              val bashScript = s""" \\
        $fastpExecutable \\
          -i ${tmpRead1.getAbsolutePath} \\
          -I ${tmpRead2.getAbsolutePath} \\
          -h ${tmpHtml.getAbsolutePath} \\
          -j ${tmpJson.getAbsolutePath} \\
          --dont_overwrite \\
          --report_title '$project: $sampleId $runId-$project-$sampleId' \\
          --thread ${resourceAllocated.cpu} """

              Exec.bash(logDiscriminator = "fastp",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              tmpRead1.delete
              tmpRead2.delete

              val nameStub = project + "." + sampleId + "." + runId

              for {
                json <- SharedFile(tmpJson,
                                   name = nameStub + ".fastp.json",
                                   deleteFile = true)
                html <- SharedFile(tmpHtml,
                                   name = nameStub + ".fastp.html",
                                   deleteFile = true)
              } yield
                FastpReport(html = html, json = json, project, sampleId, runId)
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
