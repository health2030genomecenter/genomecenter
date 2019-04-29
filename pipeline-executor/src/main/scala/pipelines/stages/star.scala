package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, JVM, StableSet}
import org.gc.pipelines.util.StableSet.syntax

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import tasks._
import tasks.circesupport._
import fileutils.TempFile
import scala.concurrent.{Future, ExecutionContext}
import java.io.File
import Executables.{star260aExecutable, star261cExecutable, picardJar}

sealed trait StarVersion {
  def executable: String
}
object StarVersion {
  case object Star261c extends StarVersion {
    def executable: String = star261cExecutable
    override def toString = "2.6.1c"
  }
  case object Star260a extends StarVersion {
    def executable: String = star260aExecutable
    override def toString = "2.6.0a"
  }

  implicit val encoder: Encoder[StarVersion] =
    deriveEncoder[StarVersion]
  implicit val decoder: Decoder[StarVersion] =
    deriveDecoder[StarVersion]
}

case class StarAlignmentInput(
    fastqs: StableSet[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    reference: StarIndexedReferenceFasta,
    gtf: SharedFile,
    readLength: Int,
    starVersion: StarVersion
) extends WithSharedFiles(fastqs.toSeq.flatMap(l =>
      List(l.read1.file, l.read2.file)) ++ Seq(reference.fasta, gtf): _*)

case class StarResult(
    finalLog: SharedFile,
    runId: RunId,
    bam: BamWithSampleMetadata
) extends WithSharedFiles(bam.files :+ finalLog: _*)

case class StarIndexedReferenceFasta(fasta: SharedFile,
                                     indexFiles: StableSet[SharedFile])
    extends WithSharedFiles(fasta) {
  def genomeFolder(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      indexFiles <- Future.traverse(indexFiles.toSeq)(_.file)
    } yield indexFiles.head.getParent
}

case class StarIndexInput(
    reference: ReferenceFasta,
    starVersion: StarVersion
) extends WithSharedFiles(reference.files: _*)

object StarAlignment {

  val indexReference =
    AsyncTask[StarIndexInput, StarIndexedReferenceFasta]("__star-index", 1) {
      case StarIndexInput(ReferenceFasta(fasta), starVersion) =>
        implicit computationEnvironment =>
          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val starExecutable = starVersion.executable

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
                  .traverse(indexFiles)(
                    f =>
                      SharedFile(
                        f,
                        fasta.name + "." + starVersion + ".star/" + f.getName))
                  .map(_.toSet)
                _ <- SharedFile(
                  tmpStdOut,
                  name = fasta.name + "." + starVersion + ".star.index.stdout")
                _ <- SharedFile(
                  tmpStdErr,
                  name = fasta.name + "." + starVersion + ".star.index.stderr")
              } yield StarIndexedReferenceFasta(fasta, indexFiles.toStable)
            }
          } yield result

    }
  val alignSample =
    AsyncTask[StarAlignmentInput, StarResult]("__star-perlane", 1) {
      case StarAlignmentInput(fastqs,
                              project,
                              sampleId,
                              runId,
                              reference,
                              gtf,
                              readLength,
                              starVersion) =>
        implicit computationEnvironment =>
          val starNumberOfThreads = math.max(1, resourceAllocated.cpu - 1) + 3

          val tmpCleanBam = TempFile.createTempFile(".bam")

          val tmpStarFolder = TempFile.createTempFile("star")
          tmpStarFolder.delete
          tmpStarFolder.mkdir

          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val lanes = fastqs.map(_.lane).toSeq.sortBy(_.toInt)

          val readGroupName = project + "." + sampleId + "." + runId + "." + lanes
            .mkString("_")
          val uniqueSampleName = project + "." + sampleId

          val platformUnit = runId + "." + lanes.mkString("_")
          val sequencingCenter = "Health2030GenomeCenter"
          val runDate: String = java.time.Instant.now.toString

          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          val fastqsSeq = fastqs.toSeq

          def fetchFiles(f: Seq[SharedFile]) = Future.traverse(f)(_.file)

          val starExecutable = starVersion.executable

          val resultF = for {
            read1 <- fetchFiles(fastqsSeq.map(_.read1.file))
            read2 <- fetchFiles(fastqsSeq.map(_.read2.file))
            localGtf <- gtf.file
            reference <- reference.genomeFolder
            result <- {

              val bashScript = s"""\\
     $starExecutable \\
        --runThreadN $starNumberOfThreads \\
        --genomeDir $reference \\
        --readFilesIn ${read1.mkString(",")} ${read2.mkString(",")} \\
        --readFilesCommand 'gunzip -c' \\
        --outFileNamePrefix ${tmpStarFolder.getAbsolutePath}/ \\
        --outStd BAM_Unsorted \\
        --outSAMtype BAM Unsorted \\
        --outSAMattributes All \\
        --outSAMmultNmax 1 \\
        --outSAMunmapped Within \\
        --outSJfilterReads Unique \\
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

              Exec.bashAudit(logDiscriminator = "star.pipes." + sampleId,
                             onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = readGroupName + "." + starVersion

              val expectedLog = new File(tmpStarFolder, "Log.out")
              val expectedFinalLog = new File(tmpStarFolder, "Log.final.out")

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".star.bam.stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".star.bam.stderr",
                                deleteFile = true)
                _ <- SharedFile(expectedLog,
                                name = nameStub + ".star.bam.Log.out",
                                deleteFile = true)
                finalLogFile <- SharedFile(
                  expectedFinalLog,
                  name = nameStub + ".star.bam.Log.final.out",
                  deleteFile = true)
                bam <- SharedFile(tmpCleanBam,
                                  name = nameStub + ".star.bam",
                                  deleteFile = true)
              } yield
                StarResult(finalLogFile,
                           runId,
                           BamWithSampleMetadata(project, sampleId, Bam(bam)))
            }
          } yield result

          resultF

    }

}

object StarIndexedReferenceFasta {
  implicit val encoder: Encoder[StarIndexedReferenceFasta] =
    deriveEncoder[StarIndexedReferenceFasta]
  implicit val decoder: Decoder[StarIndexedReferenceFasta] =
    deriveDecoder[StarIndexedReferenceFasta]
}

object StarAlignmentInput {
  implicit val encoder: Encoder[StarAlignmentInput] =
    deriveEncoder[StarAlignmentInput]
  implicit val decoder: Decoder[StarAlignmentInput] =
    deriveDecoder[StarAlignmentInput]
}

object StarResult {
  implicit val encoder: Encoder[StarResult] =
    deriveEncoder[StarResult]
  implicit val decoder: Decoder[StarResult] =
    deriveDecoder[StarResult]
}
object StarIndexInput {
  implicit val encoder: Encoder[StarIndexInput] =
    deriveEncoder[StarIndexInput]
  implicit val decoder: Decoder[StarIndexInput] =
    deriveDecoder[StarIndexInput]
}
