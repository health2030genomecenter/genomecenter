package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, JVM}
import org.gc.pipelines.util

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import tasks._
import tasks.circesupport._
import fileutils.TempFile
import scala.concurrent.{Future, ExecutionContext}
import java.io.File

case class PerLaneStarAlignmentInput(
    read1: FastQ,
    read2: FastQ,
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    lane: Lane,
    reference: StarIndexedReferenceFasta,
    gtf: SharedFile,
    readLength: Int
) extends WithSharedFiles(
      Seq(read1.file, read2.file, reference.fasta, gtf): _*)

case class StarIndexedReferenceFasta(fasta: SharedFile,
                                     indexFiles: Set[SharedFile])
    extends WithSharedFiles(fasta) {
  def genomeFolder(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      indexFiles <- Future.traverse(indexFiles)(_.file)
    } yield indexFiles.head.getParent
}

object StarAlignment {

  val indexReference =
    AsyncTask[ReferenceFasta, StarIndexedReferenceFasta]("__star-index", 1) {
      case ReferenceFasta(fasta) =>
        implicit computationEnvironment =>
          val starExecutable = extractStarExecutable()

          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          for {
            localFasta <- fasta.file
            result <- {

              val outFolder = TempFile.createTempFile(".starGenome")
              outFolder.delete
              outFolder.mkdir

              Exec.bash(logDiscriminator = "star.index",
                        onError = Exec.ThrowIfNonZero)(s"""$starExecutable \\
                    --runThreadN ${resourceAllocated.cpu} \\
                    --runMode genomeGenerate \\
                    --genomeDir ${outFolder.getAbsolutePath} \\
                    --outFileNamePrefix ${outFolder.getAbsolutePath}/ \\
                    --genomeFastaFiles ${localFasta.getAbsolutePath} \\
                    > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) \\
              """)

              val indexFiles = List(
                new File(outFolder, "chrLength.txt"),
                new File(outFolder, "chrNameLength.txt"),
                new File(outFolder, "chrName.txt"),
                new File(outFolder, "chrStart.txt"),
                new File(outFolder, "Genome"),
                new File(outFolder, "genomeParameters.txt"),
                new File(outFolder, "SA"),
                new File(outFolder, "SAindex")
              )

              for {
                indexFiles <- Future
                  .traverse(indexFiles)(f =>
                    SharedFile(f, fasta.name + ".star/" + f.getName))
                  .map(_.toSet)
                _ <- SharedFile(tmpStdOut,
                                name = fasta.name + ".star.index.stdout")
                _ <- SharedFile(tmpStdErr,
                                name = fasta.name + ".star.index.stderr")
              } yield StarIndexedReferenceFasta(fasta, indexFiles)
            }
          } yield result

    }
  val alignSingleLane =
    AsyncTask[PerLaneStarAlignmentInput, BamWithSampleMetadataPerLane](
      "__star-perlane",
      1) {
      case PerLaneStarAlignmentInput(read1,
                                     read2,
                                     project,
                                     sampleId,
                                     runId,
                                     lane,
                                     reference,
                                     gtf,
                                     readLength) =>
        implicit computationEnvironment =>
          val picardJar = BWAAlignment.extractPicardJar()

          val starExecutable = extractStarExecutable()

          val starNumberOfThreads = math.max(1, resourceAllocated.cpu - 1)

          val tmpCleanBam = TempFile.createTempFile(".bam")

          val tmpStarFolder = TempFile.createTempFile("star")
          tmpStarFolder.delete
          tmpStarFolder.mkdir

          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val readGroupName = project + "." + sampleId + "." + runId + "." + lane
          val uniqueSampleName = project + "." + sampleId

          val platformUnit = runId + "." + lane
          val sequencingCenter = "Health2030GenomeCenter"
          val runDate: String = java.time.Instant.now.toString

          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          val resultF = for {
            read1 <- read1.file.file.map(_.getAbsolutePath)
            read2 <- read2.file.file.map(_.getAbsolutePath)
            localGtf <- gtf.file
            reference <- reference.genomeFolder
            result <- {

              val bashScript = s"""\\
     $starExecutable \\
        --runThreadN $starNumberOfThreads \\
        --genomeDir $reference \\
        --readFilesIn $read1 $read2 \\
        --readFilesCommand 'gunzip -c' \\
        --outFileNamePrefix ${tmpStarFolder.getAbsolutePath}/ \\
        --outStd BAM_Unsorted \\
        --outSAMtype BAM Unsorted \\
        --outSAMattributes All \\
        --sjdbGTFfile ${localGtf.getAbsolutePath} \\
        --sjdbOverhang ${readLength - 1} \\
        --quantMode GeneCounts \\
        --twopassMode Basic \\
      \\
      2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     java ${JVM.serial} -Xmx3G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar AddOrReplaceReadGroups \\
       --OUTPUT ${tmpCleanBam.getAbsolutePath} \\
       --SORT_ORDER null \\
       --MAX_RECORDS_IN_RAM 0 \\
       --INPUT /dev/stdin \\
       --RGLB $uniqueSampleName \\
       --RGPL illumina \\
       --RGPU $platformUnit \\
       --RGSM uniqueSampleName \\
       --RGCN $sequencingCenter \\
       --RGDT $runDate \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """

              Exec.bash(logDiscriminator = "star.pipes." + sampleId,
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = readGroupName

              val expectedLog = new File(tmpStarFolder, "Log.out")
              val expectedFinalLog = new File(tmpStarFolder, "Log.final.out")

              for {
                _ <- SharedFile(tmpStdOut, name = nameStub + ".star.bam.stdout")
                _ <- SharedFile(tmpStdErr, name = nameStub + ".star.bam.stderr")
                _ <- SharedFile(expectedLog,
                                name = nameStub + ".star.bam.Log.out")
                _ <- SharedFile(expectedFinalLog,
                                name = nameStub + ".star.bam.Log.final.out")
                bam <- SharedFile(tmpCleanBam, name = nameStub + ".star.bam")
              } yield
                BamWithSampleMetadataPerLane(project,
                                             sampleId,
                                             runId,
                                             lane,
                                             Bam(bam))
            }
          } yield result

          resultF

    }

  private def extractStarExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/STAR_ffd8416315_2.6.1c_mac"
      else if (util.isLinux) "/bin/STAR_ffd8416315_2.6.1c_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "STAR_ffd8416315_2.6.1c")
      .getAbsolutePath
  }

}

object StarIndexedReferenceFasta {
  implicit val encoder: Encoder[StarIndexedReferenceFasta] =
    deriveEncoder[StarIndexedReferenceFasta]
  implicit val decoder: Decoder[StarIndexedReferenceFasta] =
    deriveDecoder[StarIndexedReferenceFasta]
}

object PerLaneStarAlignmentInput {
  implicit val encoder: Encoder[PerLaneStarAlignmentInput] =
    deriveEncoder[PerLaneStarAlignmentInput]
  implicit val decoder: Decoder[PerLaneStarAlignmentInput] =
    deriveDecoder[PerLaneStarAlignmentInput]
}
