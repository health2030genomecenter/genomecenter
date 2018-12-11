package org.gc.pipelines.stages

import org.gc.pipelines.util.{Exec}
import org.gc.pipelines.util

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import tasks._
import tasks.circesupport._
import fileutils.TempFile
import java.io.File

case class QTLToolsQuantificationInput(
    bam: CoordinateSortedBam,
    gtf: GTFFile,
    additionalCommandLineArguments: Seq[String]
) extends WithSharedFiles(bam.files ++ List(gtf.file): _*)

case class QTLToolsQuantificationResult(
    exonCounts: SharedFile,
    geneCounts: SharedFile,
    stats: SharedFile
) extends WithSharedFiles(exonCounts, geneCounts, stats)

object QTLToolsQuantification {

  val quantify =
    AsyncTask[QTLToolsQuantificationInput, QTLToolsQuantificationResult](
      "__qtltools-quant",
      1) {
      case QTLToolsQuantificationInput(bam,
                                       gtf,
                                       additionalCommandLineArguments) =>
        implicit computationEnvironment =>
          val qtltoolsExecutable = extractQTLToolsExecutable()

          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val output = TempFile.createTempFile(".out").getAbsolutePath

          val resultF = for {

            localBam <- bam.localFile
            localGtf <- gtf.file.file
            result <- {

              val bashScript = s"""\\
     $qtltoolsExecutable quan \\
        --gtf ${localGtf.getAbsolutePath}\\
        --bam ${localBam.getAbsolutePath}\\
        --out $output \\
        ${additionalCommandLineArguments.mkString(" ")} \\
      \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """

              Exec.bash(logDiscriminator = "qtltools.quant",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = bam.bam.name + ".qtltools.quant"

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".stderr",
                                deleteFile = true)
                exonCounts <- SharedFile(new File(output + ".exon.count.bed"),
                                         name = nameStub + ".exon.count.bed",
                                         deleteFile = true)
                geneCounts <- SharedFile(new File(output + ".gene.count.bed"),
                                         name = nameStub + "gene.count.bed",
                                         deleteFile = true)
                stats <- SharedFile(new File(output + ".stats"),
                                    name = nameStub + ".stats",
                                    deleteFile = true)
              } yield
                QTLToolsQuantificationResult(exonCounts, geneCounts, stats)
            }
          } yield result

          resultF

    }

  private def extractQTLToolsExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/QTLtools_9954dd57b36671a3_mac"
      else if (util.isLinux) "/bin/QTLtools_9954dd57b36671a3_linux"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "QTLtools_9954dd57b36671a3")
      .getAbsolutePath
  }

}

object QTLToolsQuantificationInput {
  implicit val encoder: Encoder[QTLToolsQuantificationInput] =
    deriveEncoder[QTLToolsQuantificationInput]
  implicit val decoder: Decoder[QTLToolsQuantificationInput] =
    deriveDecoder[QTLToolsQuantificationInput]
}

object QTLToolsQuantificationResult {
  implicit val encoder: Encoder[QTLToolsQuantificationResult] =
    deriveEncoder[QTLToolsQuantificationResult]
  implicit val decoder: Decoder[QTLToolsQuantificationResult] =
    deriveDecoder[QTLToolsQuantificationResult]
}
