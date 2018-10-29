package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{Exec, ResourceConfig, JVM, BAM}
import org.gc.pipelines.util

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

import tasks._
import tasks.circesupport._
import fileutils.TempFile
import scala.concurrent.Future
import java.io.File

case class PerLaneBWAAlignmentInput(
    read1: FastQ,
    read2: FastQ,
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    lane: Lane,
    reference: IndexedReferenceFasta,
    umi: Option[FastQ]
) extends WithSharedFiles(
      Seq(read1.file, read2.file, reference.fasta) ++ umi.toList
        .map(_.file): _*)

case class PerSampleBWAAlignmentInput(
    fastqs: Set[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    runId: RunId,
    reference: IndexedReferenceFasta
) extends WithSharedFiles(
      fastqs
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq :+ reference.fasta: _*)

case class DuplicationQCResult(markDuplicateMetrics: SharedFile)
    extends WithSharedFiles(markDuplicateMetrics)

case class MarkDuplicateResult(
    bam: BamWithSampleMetadata,
    duplicateMetric: DuplicationQCResult
) extends WithSharedFiles(bam.files ++ duplicateMetric.files: _*)

object BWAAlignment {

  val indexReference =
    AsyncTask[ReferenceFasta, IndexedReferenceFasta]("__bwa-index", 1) {
      case ReferenceFasta(fasta) =>
        implicit computationEnvironment =>
          val bwaExecutable = extractBwaExecutable()
          val picardJar = extractPicardJar()

          fasta.file.flatMap { localFastaFile =>
            val pathToFasta = localFastaFile.getAbsolutePath

            Exec.bash(logDiscriminator = "bwa.index",
                      onError = Exec.ThrowIfNonZero)(
              s"$bwaExecutable index  -a bwtsw $pathToFasta")

            Exec.bash(logDiscriminator = "fasta.dict",
                      onError = Exec.ThrowIfNonZero)(
              s"java ${JVM.serial} -Xmx1G -Dpicard.useLegacyParser=false -jar $picardJar CreateSequenceDictionary --REFERENCE $pathToFasta"
            )

            val fastaIndex = {
              import htsjdk.samtools.reference.FastaSequenceIndexCreator
              val file = new File(pathToFasta + ".fai")
              FastaSequenceIndexCreator
                .buildFromFasta(localFastaFile.toPath)
                .write(file.toPath)
              file
            }

            val dict = new File(pathToFasta.stripSuffix("fasta") + "dict")
            val bwt = new File(pathToFasta + ".bwt")
            val pac = new File(pathToFasta + ".pac")
            val ann = new File(pathToFasta + ".ann")
            val amb = new File(pathToFasta + ".amb")
            val sa = new File(pathToFasta + ".sa")

            for {
              dict <- SharedFile(dict, dict.getName)
              bwt <- SharedFile(bwt, bwt.getName)
              pac <- SharedFile(pac, pac.getName)
              ann <- SharedFile(ann, ann.getName)
              amb <- SharedFile(amb, amb.getName)
              sa <- SharedFile(sa, sa.getName)
              fai <- SharedFile(fastaIndex, fasta.name + ".fai")
            } yield
              IndexedReferenceFasta(fasta,
                                    Set(bwt, pac, ann, amb, sa, dict, fai))
          }

    }

  val alignFastqPerSample =
    AsyncTask[PerSampleBWAAlignmentInput, MarkDuplicateResult](
      "__bwa-persample",
      1) {
      case PerSampleBWAAlignmentInput(fastqs,
                                      project,
                                      sampleId,
                                      runId,
                                      reference) =>
        implicit computationEnvironment =>
          releaseResources

          def alignLane(lane: FastQPerLane) = {
            val totalReads = lane.read1.numberOfReads + lane.read2.numberOfReads + lane.umi
              .map(_.numberOfReads)
              .getOrElse(0L)

            val scratchNeeded = ((math.max(
              ResourceConfig.bwa.scratch.toDouble,
              ResourceConfig.uncompressedBamSizeBytePerRead.toDouble * (totalReads.toDouble))) / 1E6).toInt
            val resourceRequest =
              ResourceConfig.bwa.copy(
                cpuMemoryRequest = ResourceConfig.bwa.cpuMemoryRequest
                  .copy(scratch = scratchNeeded))

            log.info("BWA scratch space computed: " + scratchNeeded + "MB ")

            alignSingleLane(
              PerLaneBWAAlignmentInput(lane.read1,
                                       lane.read2,
                                       project,
                                       sampleId,
                                       runId,
                                       lane.lane,
                                       reference,
                                       lane.umi))(resourceRequest)
          }

          for {
            alignedLanes <- Future.sequence(fastqs.map(alignLane))
            merged <- mergeAndMarkDuplicate(
              BamsWithSampleMetadata(project,
                                     sampleId,
                                     runId,
                                     alignedLanes.map(_.bam)))(
              ResourceConfig.picardMergeAndMarkDuplicates)
            _ <- Future.traverse(alignedLanes.map(_.bam))(_.file.delete)

          } yield merged
    }

  val sortByCoordinateAndIndex =
    AsyncTask[Bam, CoordinateSortedBam]("__sortbam", 1) {
      case Bam(bam) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val tempFolder =
            TempFile
              .createTempFolder(".sortTempFolder")
              .getAbsolutePath
          val tmpSorted = TempFile.createTempFile(".bam")
          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")
          val javaTmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          val maxHeap = s"-Xmx${resourceAllocated.memory}m"
          val maxReads =
            (resourceAllocated.memory.toDouble * ResourceConfig.picardSamSortRecordPerMegabyteHeap).toLong

          for {
            localBam <- bam.file
            result <- {

              Exec.bash(logDiscriminator = "sortbam.sort",
                        onError = Exec.ThrowIfNonZero)(
                s"""java ${JVM.g1} $maxHeap $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar SortSam \\
                --INPUT ${localBam.getAbsolutePath} \\
                --OUTPUT ${tmpSorted.getAbsolutePath} \\
                --SORT_ORDER coordinate \\
                --TMP_DIR $tempFolder \\
                --MAX_RECORDS_IN_RAM $maxReads \\
              > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
              """)

              Exec.bash("sortbam.idx", onError = Exec.ThrowIfNonZero)(
                s"""java ${JVM.serial} $maxHeap -Dpicard.useLegacyParser=false -jar $picardJar BuildBamIndex \\
                 --INPUT ${tmpSorted.getAbsolutePath} \\
                 > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""
              )

              val expectedBai =
                new File(
                  tmpSorted.getAbsolutePath
                    .stripSuffix(".bam") + ".bai")

              val nameStub = bam.name.stripSuffix(".bam")

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".sort.stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".sort.stderr",
                                deleteFile = true)
                sortedBam <- SharedFile(tmpSorted,
                                        name = nameStub + ".sorted.bam",
                                        deleteFile = true)
                bai <- SharedFile(expectedBai,
                                  name = nameStub + ".sorted.bai",
                                  deleteFile = true)
              } yield CoordinateSortedBam(sortedBam, bai)

            }
          } yield result
    }

  val mergeAndMarkDuplicate =
    AsyncTask[BamsWithSampleMetadata, MarkDuplicateResult](
      "__merge-markduplicate",
      1) {
      case BamsWithSampleMetadata(project, sampleId, runId, bams) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val tempFolder =
            TempFile
              .createTempFolder(".markDuplicateTempFolder")
              .getAbsolutePath

          val tmpDuplicateMarkedBam = TempFile.createTempFile(".bam")
          val tmpMetricsFile = TempFile.createTempFile(".metrics")
          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val maxHeap = s"-Xmx${resourceAllocated.memory}m"
          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          for {
            localBams <- Future.sequence(bams.map(_.file.file))
            result <- {

              localBams.foreach { localBam =>
                require(
                  BAM.getSortOrder(localBam) == htsjdk.samtools.SAMFileHeader.SortOrder.queryname,
                  "Error in pipeline. Input bam of mark duplicate should be queryname sorted."
                )
              }

              val inputFlags = localBams
                .map(_.getAbsolutePath)
                .mkString("--INPUT ", " --INPUT ", "")

              val bashScript = s"""
        java ${JVM.g1} $maxHeap $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar MarkDuplicates \\
          $inputFlags \\
          --OUTPUT ${tmpDuplicateMarkedBam.getAbsolutePath} \\
          --METRICS_FILE ${tmpMetricsFile.getAbsolutePath} \\
          --OPTICAL_DUPLICATE_PIXEL_DISTANCE=250 \\
          --CREATE_INDEX false \\
          --COMPRESSION_LEVEL 1 \\
          --MAX_RECORDS_IN_RAM 0 \\
          --TMP_DIR $tempFolder \\
          > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)        
        """

              Exec.bash(logDiscriminator = "markduplicates." + sampleId,
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = project + "." + sampleId + "." + runId

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".mdup.bam.stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".mdup.bam.stderr",
                                deleteFile = true)
                duplicateMetric <- SharedFile(
                  tmpMetricsFile,
                  name = nameStub + ".mdup.markDuplicateMetrics",
                  deleteFile = true)
                bam <- SharedFile(tmpDuplicateMarkedBam,
                                  name = nameStub + ".mdup.bam",
                                  deleteFile = true)
              } yield
                MarkDuplicateResult(
                  BamWithSampleMetadata(project, sampleId, runId, Bam(bam)),
                  DuplicationQCResult(duplicateMetric))

            }
          } yield result

    }

  val alignSingleLane =
    AsyncTask[PerLaneBWAAlignmentInput, BamWithSampleMetadataPerLane](
      "__bwa-perlane",
      1) {
      case PerLaneBWAAlignmentInput(read1,
                                    read2,
                                    project,
                                    sampleId,
                                    runId,
                                    lane,
                                    reference,
                                    maybeUmi) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val bwaExecutable = extractBwaExecutable()

          val bwaNumberOfThreads = math.max(1, resourceAllocated.cpu - 1)

          val markAdapterMetricsFileOutput =
            TempFile.createTempFile(".markAdapterMetrics").getAbsolutePath

          val markAdapterTempFolder =
            TempFile.createTempFolder(".markAdapterTempFolder").getAbsolutePath

          val samToFastqTempFolder =
            TempFile.createTempFolder(".samToFastqTempFolder")

          val mergeBamAlignmentTempFolder =
            TempFile.createTempFolder(".mergeBamAlignmentTempFolder")

          val unmappedBamTempFolder =
            TempFile.createTempFolder(".unmappedBamFastqToSamTempFolder")

          val tmpCleanBam = TempFile.createTempFile(".bam")
          val tmpIntermediateUnmappedBam =
            TempFile.createTempFile(".unmappedBam")

          val tmpStdOut = TempFile.createTempFile(".stdout")
          val tmpStdErr = TempFile.createTempFile(".stderr")

          val readGroupName = project + "." + sampleId + "." + runId + "." + lane
          val uniqueSampleName = project + "." + sampleId

          val platformUnit = runId + "." + lane
          val sequencingCenter = "Health2030GenomeCenter"
          val runDate: String = java.time.Instant.now.toString

          val maxHeap = s"-Xmx${resourceAllocated.memory}m"
          val maxReads =
            (resourceAllocated.memory.toDouble * ResourceConfig.picardSamSortRecordPerMegabyteHeap).toLong

          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          val resultF = for {
            read1 <- read1.file.file.map(_.getAbsolutePath)
            read2 <- read2.file.file.map(_.getAbsolutePath)
            umi <- maybeUmi match {
              case Some(umi) =>
                umi.file.file.map(_.getAbsolutePath).map(Some(_))
              case None => Future.successful(None)
            }
            reference <- reference.localFile
            result <- {

              umi match {
                case None =>
                  val fastqToUnmappedBam = s"""\\
        java ${JVM.serial} $maxHeap $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
                --FASTQ $read1 \\
                --FASTQ2 $read2 \\
                --OUTPUT ${tmpIntermediateUnmappedBam.getAbsolutePath} \\
                --QUIET true \\
                --SORT_ORDER queryname \\
                --COMPRESSION_LEVEL 1 \\
                --READ_GROUP_NAME $readGroupName \\
                --SAMPLE_NAME $uniqueSampleName \\
                --LIBRARY_NAME $uniqueSampleName \\
                --PLATFORM_UNIT $platformUnit \\
                --PLATFORM illumina \\
                --SEQUENCING_CENTER  $sequencingCenter \\
                --TMP_DIR ${unmappedBamTempFolder.getAbsolutePath} \\
                --MAX_RECORDS_IN_RAM $maxReads \\
                --RUN_DATE $runDate 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
              """

                  Exec.bash(logDiscriminator = "bwa.fastq2ubam." + sampleId,
                            onError = Exec.ThrowIfNonZero)(fastqToUnmappedBam)
                case Some(umi) =>
                  val umiProcessor = extractUmiProcessorJar()
                  val fastqToUnmappedBam = s"""\\
        java ${JVM.serial} -Xmx2G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
                --FASTQ $read1 \\
                --FASTQ2 $read2 \\
                --OUTPUT /dev/stdout \\
                --SORT_ORDER unsorted \\
                --MAX_RECORDS_IN_RAM 0 \\
                --COMPRESSION_LEVEL 1 \\
                --READ_GROUP_NAME $readGroupName \\
                --SAMPLE_NAME $uniqueSampleName \\
                --LIBRARY_NAME $uniqueSampleName \\
                --PLATFORM_UNIT $platformUnit \\
                --PLATFORM illumina \\
                --SEQUENCING_CENTER  $sequencingCenter \\
                --TMP_DIR ${unmappedBamTempFolder.getAbsolutePath} \\
                --RUN_DATE $runDate 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
                \\
        java ${JVM.serial} -Xmx2G $tmpDir -jar $umiProcessor $umi \\
              2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
                 \\
        java ${JVM.g1} $maxHeap $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar SortSam \\
                --INPUT /dev/stdin/ \\
                --OUTPUT ${tmpIntermediateUnmappedBam.getAbsolutePath} \\
                --SORT_ORDER queryname \\
                --TMP_DIR ${unmappedBamTempFolder.getAbsolutePath} \\
                --MAX_RECORDS_IN_RAM $maxReads \\
                2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
              """

                  Exec.bash(logDiscriminator = "bwa.fastq2ubam_umi." + sampleId,
                            onError = Exec.ThrowIfNonZero)(fastqToUnmappedBam)
              }

              log.info(s"Fastq of $sampleId sorted.")

              val bashScript = s"""\\
     java ${JVM.serial} -Xmx4G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar MarkIlluminaAdapters \\
       --INPUT ${tmpIntermediateUnmappedBam.getAbsolutePath} \\
       --OUTPUT /dev/stdout \\
       --QUIET true \\
       --METRICS $markAdapterMetricsFileOutput \\
       --TMP_DIR $markAdapterTempFolder 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
    java ${JVM.serial} -Xmx3G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar SamToFastq \\
      --INPUT /dev/stdin \\
      --FASTQ /dev/stdout \\
      --QUIET true \\
      --CLIPPING_ATTRIBUTE XT \\
      --CLIPPING_ACTION 2 \\
      --INTERLEAVE true \\
      --INCLUDE_NON_PF_READS true \\
      --TMP_DIR $samToFastqTempFolder 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     $bwaExecutable mem -M -t $bwaNumberOfThreads -p $reference /dev/stdin 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2) | \\
     \\
     java ${JVM.serial} -Xmx3G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar MergeBamAlignment \\
       --REFERENCE_SEQUENCE $reference \\
       --UNMAPPED_BAM ${tmpIntermediateUnmappedBam.getAbsolutePath} \\
       --ALIGNED_BAM /dev/stdin \\
       --OUTPUT ${tmpCleanBam.getAbsolutePath} \\
       --CREATE_INDEX false \\
       --ADD_MATE_CIGAR true \\
       --SORT_ORDER queryname \\
       --CLIP_ADAPTERS false \\
       --CLIP_OVERLAPPING_READS true \\
       --INCLUDE_SECONDARY_ALIGNMENTS true \\
       --MAX_INSERTIONS_OR_DELETIONS -1 \\
       --PRIMARY_ALIGNMENT_STRATEGY MostDistant \\
       --ATTRIBUTES_TO_RETAIN XS \\
       --MAX_RECORDS_IN_RAM 0 \\
       --TMP_DIR $mergeBamAlignmentTempFolder \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)        
      """

              Exec.bash(logDiscriminator = "bwa.pipes." + sampleId,
                        onError = Exec.ThrowIfNonZero)(bashScript)

              log.info(
                s"Deleting intermediate unmapped bam $tmpIntermediateUnmappedBam .")
              tmpIntermediateUnmappedBam.delete

              val nameStub = readGroupName

              for {
                _ <- SharedFile(tmpStdOut,
                                name = nameStub + ".bam.stdout",
                                deleteFile = true)
                _ <- SharedFile(tmpStdErr,
                                name = nameStub + ".bam.stderr",
                                deleteFile = true)
                bam <- SharedFile(tmpCleanBam,
                                  name = nameStub + ".bam",
                                  deleteFile = true)
              } yield
                BamWithSampleMetadataPerLane(project,
                                             sampleId,
                                             runId,
                                             lane,
                                             Bam(bam))
            }
          } yield result

          resultF.andThen {
            case _ =>
              if (tmpIntermediateUnmappedBam.canRead) {
                tmpIntermediateUnmappedBam.delete
              }
          }

          resultF

    }

  def extractPicardJar(): String =
    fileutils.TempFile
      .getExecutableFromJar("/bin/picard_2.8.14.jar", "picard_2.8.14.jar")
      .getAbsolutePath

  def extractUmiProcessorJar(): String =
    fileutils.TempFile
      .getExecutableFromJar("/umiprocessor", "umiprocessor")
      .getAbsolutePath

  private def extractBwaExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/bwa_0.7.17-r1188_mac"
      else if (util.isLinux) "/bin/bwa_0.7.17-r1188_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "bwa_0.7.17-r1188")
      .getAbsolutePath
  }

}

object PerLaneBWAAlignmentInput {
  implicit val encoder: Encoder[PerLaneBWAAlignmentInput] =
    deriveEncoder[PerLaneBWAAlignmentInput]
  implicit val decoder: Decoder[PerLaneBWAAlignmentInput] =
    deriveDecoder[PerLaneBWAAlignmentInput]
}

object PerSampleBWAAlignmentInput {
  implicit val encoder: Encoder[PerSampleBWAAlignmentInput] =
    deriveEncoder[PerSampleBWAAlignmentInput]
  implicit val decoder: Decoder[PerSampleBWAAlignmentInput] =
    deriveDecoder[PerSampleBWAAlignmentInput]
}

object MarkDuplicateResult {
  implicit val encoder: Encoder[MarkDuplicateResult] =
    deriveEncoder[MarkDuplicateResult]
  implicit val decoder: Decoder[MarkDuplicateResult] =
    deriveDecoder[MarkDuplicateResult]
}

object DuplicationQCResult {
  implicit val encoder: Encoder[DuplicationQCResult] =
    deriveEncoder[DuplicationQCResult]
  implicit val decoder: Decoder[DuplicationQCResult] =
    deriveDecoder[DuplicationQCResult]
}
