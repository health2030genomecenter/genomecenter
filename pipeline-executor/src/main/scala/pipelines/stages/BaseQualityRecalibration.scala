package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import fileutils.TempFile
import org.gc.pipelines.util.{Exec, GATK}
import java.io.File

case class TrainBQSRInput(bam: CoordinateSortedBam,
                          reference: IndexedReferenceFasta,
                          knownSites: Set[VCF])
    extends WithSharedFiles(
      bam.files ++ reference.files ++ knownSites.toSeq.flatMap(_.files): _*)

case class ApplyBQSRInput(bam: CoordinateSortedBam,
                          reference: IndexedReferenceFasta,
                          bqsrTable: BQSRTable)
    extends WithSharedFiles(bam.files ++ reference.files ++ bqsrTable.files: _*)

object BaseQualityScoreRecalibration {

  val trainBQSR = AsyncTask[TrainBQSRInput, BQSRTable]("__bqsr-train", 1) {
    case TrainBQSRInput(bam, reference, knownSites) =>
      implicit computationEnvironment =>
        val maxHeap = s"-Xmx${resourceAllocated.memory}m"

        for {
          localBam <- bam.bam.file
          reference <- reference.localFile
          knownSites <- Future.traverse(knownSites)(_.localFile)
          result <- {

            val output = TempFile.createTempFile(".bsqr.report")
            val gatkJar: String = extractGatkJar()
            val knownSitesArguments = knownSites
              .map(_.getAbsolutePath)
              .mkString(" --known-sites ", " --known-sites ", "")
            val tmpStdOut = TempFile.createTempFile(".stdout")
            val tmpStdErr = TempFile.createTempFile(".stderr")
            val bashScript = s""" \\
              java $maxHeap ${GATK.javaArguments} -jar $gatkJar  BaseRecalibrator \\
                -R ${reference.getAbsolutePath} \\
                -I ${localBam.getAbsolutePath} \\
                -O ${output.getAbsolutePath} \\
                --use-original-qualities \\
                $knownSitesArguments \\
                > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""

            Exec.bash(logDiscriminator = "bqsr.train",
                      onError = Exec.ThrowIfNonZero)(bashScript)
            for {
              _ <- SharedFile(tmpStdOut,
                              name = bam.bam.name + ".bqsr.train.stdout",
                              deleteFile = true)
              _ <- SharedFile(tmpStdErr,
                              name = bam.bam.name + ".bqsr.train.stderr",
                              deleteFile = true)
              table <- SharedFile(output,
                                  bam.bam.name + ".bqsr.tab",
                                  deleteFile = true)
                .map(BQSRTable(_))
            } yield table

          }
        } yield result
  }

  val applyBQSR =
    AsyncTask[ApplyBQSRInput, CoordinateSortedBam]("__bqsr-apply", 1) {
      case ApplyBQSRInput(bam, reference, bqsrTable) =>
        implicit computationEnvironment =>
          val maxHeap = s"-Xmx${resourceAllocated.memory}m"

          for {
            localBam <- bam.bam.file
            reference <- reference.localFile
            bqsrTable <- bqsrTable.file.file
            result <- {

              val outputBam = TempFile.createTempFile(".bam")
              val expectedBai =
                new File(outputBam.getAbsolutePath.stripSuffix("bam") + "bai")
              val gatkJar: String = extractGatkJar()
              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val bashScript = s""" \\
              java $maxHeap ${GATK.javaArguments} -jar $gatkJar ApplyBQSR \\
                -R ${reference.getAbsolutePath} \\
                -I ${localBam.getAbsolutePath} \\
                -O ${outputBam.getAbsolutePath} \\
                --use-original-qualities \\
                --add-output-sam-program-record \\
                --create-output-bam-index \\
                --create-output-bam-md5 \\
                --emit-original-quals \\
                -bqsr ${bqsrTable.getAbsolutePath} \\
                > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                """

              Exec.bash(logDiscriminator = "bqsr.apply",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val outputFileNameRoot = bam.bam.name.stripSuffix("bam")
              for {
                _ <- SharedFile(tmpStdOut,
                                name = outputFileNameRoot + "bqsr.apply.stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = outputFileNameRoot + "bqsr.apply.stderr",
                                deleteFile = true)
                bai <- SharedFile(expectedBai,
                                  outputFileNameRoot + "bqsr.bai",
                                  deleteFile = true)
                recalibrated <- SharedFile(outputBam,
                                           outputFileNameRoot + "bqsr.bam",
                                           deleteFile = true)
                _ <- bam.bam.delete
                _ <- bam.bai.delete
              } yield {
                CoordinateSortedBam(recalibrated, bai)
              }

            }
          } yield result
    }

  private def extractGatkJar(): String =
    fileutils.TempFile
      .getExecutableFromJar("/bin/gatk-package-4.0.9.0-local.jar",
                            "gatk-package-4.0.9.0-local.jar")
      .getAbsolutePath

}

object TrainBQSRInput {
  implicit val encoder: Encoder[TrainBQSRInput] =
    deriveEncoder[TrainBQSRInput]
  implicit val decoder: Decoder[TrainBQSRInput] =
    deriveDecoder[TrainBQSRInput]
}

object ApplyBQSRInput {
  implicit val encoder: Encoder[ApplyBQSRInput] =
    deriveEncoder[ApplyBQSRInput]
  implicit val decoder: Decoder[ApplyBQSRInput] =
    deriveDecoder[ApplyBQSRInput]
}
