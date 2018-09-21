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

  val trainBQSR = AsyncTask[TrainBQSRInput, BQSRTable]("bqsr-train", 1) {
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
            val bashScript = s""" \\
              java $maxHeap ${GATK.javaArguments} -jar $gatkJar  BaseRecalibrator \\
                -R ${reference.getAbsolutePath} \\
                -I ${localBam.getAbsolutePath} \\
                -O ${output.getAbsolutePath} \\
                --use-original-qualities \\
                $knownSitesArguments """

            Exec.bash(logDiscriminator = "bqsr.train",
                      onError = Exec.ThrowIfNonZero)(bashScript)

            SharedFile(output, bam.bam.name + ".bqsr.tab").map(BQSRTable(_))

          }
        } yield result
  }

  val applyBQSR =
    AsyncTask[ApplyBQSRInput, CoordinateSortedBam]("bqsr-apply", 1) {
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
                -bqsr ${bqsrTable.getAbsolutePath}
                """

              Exec.bash(logDiscriminator = "bqsr.apply",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val outputFileNameRoot = bam.bam.name.stripSuffix("bam")
              for {
                bai <- SharedFile(expectedBai, outputFileNameRoot + "bqsr.bai")
                bam <- SharedFile(outputBam, outputFileNameRoot + "bqsr.bam")
              } yield {
                CoordinateSortedBam(bam, bai)
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
