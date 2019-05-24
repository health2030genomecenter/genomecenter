package org.gc.pipelines.stages

import org.gc.pipelines.util.{Exec, Files}

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import tasks._
import tasks.circesupport._
import java.io.File
import Executables.qtlToolsExecutable

case class QTLToolsQuantificationInput(
    bam: CoordinateSortedBam,
    gtf: GTFFile,
    additionalCommandLineArguments: Seq[String]
)

case class QTLToolsQuantificationResult(
    exonCounts: SharedFile,
    geneCounts: SharedFile,
    stats: SharedFile,
    exonRpkms: SharedFile,
    geneRpkms: SharedFile
) extends WithSharedFiles(
      mutables = List(exonCounts, geneCounts, stats, exonRpkms, geneRpkms))

object QTLToolsQuantification {

  val quantify =
    AsyncTask[QTLToolsQuantificationInput, QTLToolsQuantificationResult](
      "__qtltools-quant",
      4) {
      case QTLToolsQuantificationInput(bam,
                                       gtf,
                                       additionalCommandLineArguments) =>
        implicit computationEnvironment =>
          val tmpStdOut = Files.createTempFile(".stdout")
          val tmpStdErr = Files.createTempFile(".stderr")

          val output = Files.createTempFile(".out").getAbsolutePath

          val resultF = for {

            localBam <- bam.localFile
            localGtf <- gtf.file.file
            result <- {

              val bashScript = s"""\\
     $qtlToolsExecutable quan \\
        --gtf ${localGtf.getAbsolutePath}\\
        --bam ${localBam.getAbsolutePath}\\
        --rpkm \\
        --out $output \\
        --no-hash \\
        ${additionalCommandLineArguments.mkString(" ")} \\
      \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """

              Exec.bashAudit(logDiscriminator = "qtltools.quant",
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
                exonRpkms <- SharedFile(new File(output + ".exon.rpkm.bed"),
                                        name = nameStub + ".exon.rpkm.bed",
                                        deleteFile = true)
                geneCounts <- SharedFile(new File(output + ".gene.count.bed"),
                                         name = nameStub + ".gene.count.bed",
                                         deleteFile = true)
                geneRpkms <- SharedFile(new File(output + ".gene.rpkm.bed"),
                                        name = nameStub + ".gene.rpkm.bed",
                                        deleteFile = true)
                stats <- SharedFile(new File(output + ".stats"),
                                    name = nameStub + ".stats",
                                    deleteFile = true)
              } yield
                QTLToolsQuantificationResult(exonCounts,
                                             geneCounts,
                                             stats,
                                             exonRpkms,
                                             geneRpkms)
            }
          } yield result

          resultF

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
