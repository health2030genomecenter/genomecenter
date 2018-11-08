package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import fileutils.TempFile
import org.gc.pipelines.util.{Exec, GATK, JVM, ResourceConfig, Fasta}
import scala.collection.JavaConverters._
import java.io.File

case class TrainBQSRInput(bam: CoordinateSortedBam,
                          reference: IndexedReferenceFasta,
                          knownSites: Set[VCF])
    extends WithSharedFiles(
      bam.files ++ reference.files ++ knownSites.toSeq.flatMap(_.files): _*)

case class TrainBQSRInputScatteredPiece(bam: CoordinateSortedBam,
                                        reference: IndexedReferenceFasta,
                                        knownSites: Set[VCF],
                                        interval: String)
    extends WithSharedFiles(
      bam.files ++ reference.files ++ knownSites.toSeq.flatMap(_.files): _*)

case class ApplyBQSRInput(bam: CoordinateSortedBam,
                          reference: IndexedReferenceFasta,
                          bqsrTable: BQSRTable)
    extends WithSharedFiles(bam.files ++ reference.files ++ bqsrTable.files: _*)

case class ApplyBQSRInputScatteredPiece(bam: CoordinateSortedBam,
                                        reference: IndexedReferenceFasta,
                                        bqsrTable: BQSRTable,
                                        interval: String)
    extends WithSharedFiles(bam.files ++ reference.files ++ bqsrTable.files: _*)

object BaseQualityScoreRecalibration {

  def createIntervals(dict: File): Seq[String] = {
    val samSequenceDictionary = Fasta.parseDict(dict)
    samSequenceDictionary.getSequences.asScala.toList.map { sequence =>
      sequence.getSequenceName
    } :+ "unmapped"
  }

  val trainBQSR =
    AsyncTask[TrainBQSRInput, BQSRTable]("__bqsr-train", 1) {
      case TrainBQSRInput(bam, reference, knownSites) =>
        implicit computationEnvironment =>
          releaseResources
          def inScatteredFolder[T] =
            appendToFilePrefix[T](Seq("bqsr-train-scattered"))

          for {
            dict <- reference.dict
            intervals = BaseQualityScoreRecalibration.createIntervals(dict)
            scattered <- Future.traverse(intervals) { interval =>
              inScatteredFolder { implicit computationEnvironment =>
                trainBQSRPiece(
                  TrainBQSRInputScatteredPiece(bam,
                                               reference,
                                               knownSites,
                                               interval))(
                  ResourceConfig.trainBqsr)
              }
            }
            localTables <- Future.traverse(scattered)(_.file.file)
            gatheredTables <- {

              val output = TempFile.createTempFile(".bsqr.report")
              val gatkJar: String = extractGatkJar()
              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val input = " -I " + localTables.mkString(" -I ")

              Exec.bash(logDiscriminator = "bqsr.train",
                        onError = Exec.ThrowIfNonZero)(
                s"""\\
              java ${JVM.serial} -Xmx3G ${GATK.javaArguments(
                  compressionLevel = 1)} -jar $gatkJar GatherBQSRReports \\
                $input\\
                -O ${output.getAbsolutePath} \\
                > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""
              )
              for {
                _ <- SharedFile(
                  tmpStdOut,
                  name = bam.bam.name + ".bqsr.train.gather.stdout",
                  deleteFile = true)
                _ <- SharedFile(
                  tmpStdErr,
                  name = bam.bam.name + ".bqsr.train.gather.stderr",
                  deleteFile = true)
                table <- SharedFile(output,
                                    bam.bam.name + ".bqsr.gather.tab",
                                    deleteFile = true)
                  .map(BQSRTable(_))
              } yield table

            }
          } yield gatheredTables
    }

  val trainBQSRPiece =
    AsyncTask[TrainBQSRInputScatteredPiece, BQSRTable]("__bqsr-train_scattered",
                                                       1) {
      case TrainBQSRInputScatteredPiece(bam, reference, knownSites, interval) =>
        implicit computationEnvironment =>
          val maxHeap = JVM.maxHeap
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
              java ${JVM.g1} $maxHeap ${GATK
                .javaArguments(compressionLevel = 1)} -jar $gatkJar  BaseRecalibrator \\
                -R ${reference.getAbsolutePath} \\
                -I ${localBam.getAbsolutePath} \\
                -O ${output.getAbsolutePath} \\
                -L $interval \\
                --use-original-qualities \\
                $knownSitesArguments \\
                > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""

              Exec.bash(logDiscriminator = "bqsr.train",
                        onError = Exec.ThrowIfNonZero)(bashScript)
              for {
                _ <- SharedFile(
                  tmpStdOut,
                  name = bam.bam.name + "." + interval + ".bqsr.train.stdout",
                  deleteFile = true)
                _ <- SharedFile(
                  tmpStdErr,
                  name = bam.bam.name + "." + interval + ".bqsr.train.stderr",
                  deleteFile = true)
                table <- SharedFile(output,
                                    bam.bam.name + "." + interval + ".bqsr.tab",
                                    deleteFile = true)
                  .map(BQSRTable(_))
              } yield table

            }
          } yield result
    }

  val applyBQSR =
    AsyncTask[ApplyBQSRInput, CoordinateSortedBam]("__bqsr-apply", 1) {
      case ApplyBQSRInput(bam, reference, knownSites) =>
        implicit computationEnvironment =>
          releaseResources

          def inScatteredFolder[T] =
            appendToFilePrefix[T](Seq("bqsr-apply-scattered"))

          for {
            dict <- reference.dict
            intervals = BaseQualityScoreRecalibration.createIntervals(dict)
            scattered <- Future.traverse(intervals) { interval =>
              inScatteredFolder { implicit computationEnvironment =>
                applyBQSRPiece(
                  ApplyBQSRInputScatteredPiece(bam,
                                               reference,
                                               knownSites,
                                               interval))(
                  ResourceConfig.applyBqsr)
              }
            }
            localBams <- Future.traverse(scattered)(_.localFile)
            gathered <- {

              val output = TempFile.createTempFile(".bsqr.bam")
              val picardJar: String = BWAAlignment.extractPicardJar()
              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val input = " --INPUT " + localBams.mkString(" -INPUT ")

              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              Exec.bash(logDiscriminator = "bqsrl.apply.gather",
                        onError = Exec.ThrowIfNonZero)(
                s"""java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar GatherBamFiles \\
                $input \\
                --OUTPUT ${output.getAbsolutePath} \\
                --CREATE_INDEX true \\
              > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
              """)

              val expectedBai =
                new File(
                  output.getAbsolutePath
                    .stripSuffix(".bam") + ".bai")

              for {
                _ <- SharedFile(
                  tmpStdOut,
                  name = bam.bam.name + ".bqsr.apply.gather.stdout",
                  deleteFile = true)
                _ <- SharedFile(
                  tmpStdErr,
                  name = bam.bam.name + ".bqsr.apply.gather.stderr",
                  deleteFile = true)
                gatheredBam <- SharedFile(output,
                                          bam.bam.name + ".bqsr.bam",
                                          deleteFile = true)
                bai <- SharedFile(expectedBai,
                                  bam.bam.name + ".bqsr.bai",
                                  deleteFile = true)
                _ <- Future.traverse(scattered)(_.bam.delete)
              } yield CoordinateSortedBam(gatheredBam, bai)

            }
          } yield gathered
    }

  val applyBQSRPiece =
    AsyncTask[ApplyBQSRInputScatteredPiece, CoordinateSortedBam](
      "__bqsr-apply-scattered",
      1) {
      case ApplyBQSRInputScatteredPiece(bam, reference, bqsrTable, interval) =>
        implicit computationEnvironment =>
          val maxHeap = JVM.maxHeap

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
              java ${JVM.g1} $maxHeap ${GATK.javaArguments(compressionLevel = 5)} -jar $gatkJar ApplyBQSR \\
                -R ${reference.getAbsolutePath} \\
                -I ${localBam.getAbsolutePath} \\
                -O ${outputBam.getAbsolutePath} \\
                -L $interval \\
                --use-original-qualities \\
                --add-output-sam-program-record \\
                --create-output-bam-index \\
                --create-output-bam-md5 \\
                -bqsr ${bqsrTable.getAbsolutePath} \\
                > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                """

              Exec.bash(logDiscriminator = "bqsr.apply",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val outputFileNameRoot = bam.bam.name.stripSuffix("bam")
              for {
                _ <- SharedFile(
                  tmpStdOut,
                  name = outputFileNameRoot + s"bqsr.apply.$interval.stdout",
                  deleteFile = true)
                _ <- SharedFile(
                  tmpStdErr,
                  name = outputFileNameRoot + s"bqsr.apply.$interval.stderr",
                  deleteFile = true)
                bai <- SharedFile(expectedBai,
                                  outputFileNameRoot + s"bqsr.$interval.bai",
                                  deleteFile = true)
                recalibrated <- SharedFile(
                  outputBam,
                  outputFileNameRoot + s"bqsr.$interval.bam",
                  deleteFile = true)
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

object TrainBQSRInputScatteredPiece {
  implicit val encoder: Encoder[TrainBQSRInputScatteredPiece] =
    deriveEncoder[TrainBQSRInputScatteredPiece]
  implicit val decoder: Decoder[TrainBQSRInputScatteredPiece] =
    deriveDecoder[TrainBQSRInputScatteredPiece]
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

object ApplyBQSRInputScatteredPiece {
  implicit val encoder: Encoder[ApplyBQSRInputScatteredPiece] =
    deriveEncoder[ApplyBQSRInputScatteredPiece]
  implicit val decoder: Decoder[ApplyBQSRInputScatteredPiece] =
    deriveDecoder[ApplyBQSRInputScatteredPiece]
}
