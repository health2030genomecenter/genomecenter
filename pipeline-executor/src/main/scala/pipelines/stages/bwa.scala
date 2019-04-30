package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.{
  Exec,
  ResourceConfig,
  JVM,
  BAM,
  Files,
  FastQHelpers
}
import FastQHelpers.VirtualPointerInterval

import org.gc.pipelines.util.{StableSet, traverseAll}
import org.gc.pipelines.util.StableSet.syntax

import Executables.{
  picardJar,
  samtoolsExecutable,
  bwaExecutable,
  umiProcessorJar
}

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
    partition: PartitionId,
    reference: IndexedReferenceFasta,
    umi: Option[FastQ],
    interval: Option[IntervalTriplet]
) extends WithSharedFiles(
      Seq(read1.file, read2.file, reference.fasta) ++ umi.toList
        .map(_.file): _*)

case class SplitFastQsInput(fastqs: StableSet[FastQPerLane],
                            maxReadsPerFastQ: Long)
    extends WithSharedFiles(
      fastqs.toSeq
        .flatMap(fq =>
          List(fq.read1.file, fq.read2.file) ++ fq.umi.toSeq.map(_.file)): _*
    )
case class SplitIntervalsResult(
    intervals: StableSet[(FastQ, Seq[VirtualPointerInterval])])
    extends WithSharedFiles(intervals.toSeq.flatMap(_._1.files): _*)

case class PerSampleBWAAlignmentInput(
    fastqs: StableSet[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    reference: IndexedReferenceFasta
) extends WithSharedFiles(
      (fastqs
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq :+ reference.fasta): _*)

case class PerSampleBWAAlignmentResult(
    alignedLanes: StableSet[BamWithSampleMetadataPerLane]
) extends WithSharedFiles(alignedLanes.toSeq.flatMap(_.files): _*)

case class DuplicationQCResult(markDuplicateMetrics: SharedFile)
    extends WithSharedFiles(markDuplicateMetrics)

/* This file is not extending WithSharedFiles
 *
 * File integrity checks are not performed
 * and overwriting these results is possible
 */
case class MarkDuplicateResult(
    bam: BamWithSampleMetadata,
    duplicateMetric: DuplicationQCResult
) extends WithSharedFiles(bam.files ++ duplicateMetric.files: _*)

case class ComputeSplitIntervalInput(fq: FastQ, maxReads: Long)
    extends WithSharedFiles(fq.files: _*)

case class IntervalTriplet(read1: VirtualPointerInterval,
                           read2: VirtualPointerInterval,
                           umi: Option[VirtualPointerInterval])

object BWAAlignment {

  val computeSplitIntervals =
    AsyncTask[ComputeSplitIntervalInput, Seq[VirtualPointerInterval]](
      "__split-interval",
      1) {
      case ComputeSplitIntervalInput(FastQ(file, _, _), maxReads) =>
        implicit computationEnvironment =>
          for {
            file <- file.file
          } yield {
            FastQHelpers.indexFastQSplits(file, maxReads)
          }

    }

  val computeManySplitIntervals =
    AsyncTask[SplitFastQsInput, SplitIntervalsResult]("__split-fastqs", 1) {
      case SplitFastQsInput(fastqs, maxPerSplit) =>
        implicit computationEnvironment =>
          releaseResources
          val allFastQs = fastqs.toSeq.flatMap(_.fastqs)
          for {
            intervalsPerFastQs <- traverseAll(allFastQs) { fastq =>
              if (fastq.numberOfReads <= maxPerSplit)
                Future.successful(fastq -> Nil)
              else
                for {
                  intervals <- computeSplitIntervals(
                    ComputeSplitIntervalInput(fastq, maxPerSplit))(
                    ResourceConfig.minimal)
                } yield (fastq, intervals)

            }
          } yield SplitIntervalsResult(intervalsPerFastQs.toSet.toStable)

    }

  val indexReference =
    AsyncTask[ReferenceFasta, IndexedReferenceFasta]("__bwa-index", 1) {
      case ReferenceFasta(fasta) =>
        implicit computationEnvironment =>
          fasta.file.flatMap { localFastaFile =>
            val pathToFasta = localFastaFile.getAbsolutePath

            Exec.bash(logDiscriminator = "bwa.index",
                      onError = Exec.ThrowIfNonZero)(
              s"$bwaExecutable index  -a bwtsw $pathToFasta")

            val dictionaryPath = pathToFasta
              .split('.')
              .dropRight(1)
              .mkString(".") + ".dict"
            new File(dictionaryPath).delete

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

            val dict = new File(dictionaryPath)
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
              IndexedReferenceFasta(
                fasta,
                StableSet(bwt, pac, ann, amb, sa, dict, fai))
          }

    }

  val alignFastqPerSample =
    AsyncTask[PerSampleBWAAlignmentInput, PerSampleBWAAlignmentResult](
      "__bwa-persample",
      1) {
      case PerSampleBWAAlignmentInput(fastqs, project, sampleId, reference) =>
        implicit computationEnvironment =>
          releaseResources

          val maxReadsPerChunk = ResourceConfig.bwaMaxReadsPerChunk

          def alignLane(lane: FastQPerLane,
                        intervals: Map[FastQ, Seq[VirtualPointerInterval]]) = {
            val read1Intervals = intervals(lane.read1)
            val read2Intervals = intervals(lane.read2)
            val umiIntervals = lane.umi.map(umi => intervals(umi))
            require(
              read1Intervals.size == read2Intervals.size,
              "Programmer error, number of reads thus number of intervals should be the same.")
            if (read1Intervals.isEmpty) {
              log.debug(
                s"Aligning lane of $project $sampleId in one piece (no interval based scatter).")
              alignLaneInterval(lane, None, 0).map(List(_))
            } else {
              log.debug(
                s"Aligning lane of $project $sampleId in ${read1Intervals.size} pieces.")

              val zipped = umiIntervals match {
                case None =>
                  (read1Intervals zip read2Intervals).map(iv => (iv, None))
                case Some(umiIntervals) =>
                  read1Intervals zip read2Intervals zip umiIntervals.map(
                    Some(_))
              }
              traverseAll(zipped.zipWithIndex) {
                case (((read1, read2), umi), idx) =>
                  val triplet =
                    IntervalTriplet(read1 = read1, read2 = read2, umi)
                  alignLaneInterval(lane, Some(triplet), idx)
              }
            }

          }

          def alignLaneInterval(lane: FastQPerLane,
                                intervals: Option[IntervalTriplet],
                                intervalIdx: Int) = {
            val totalReads = intervals match {
              case None =>
                lane.read1.numberOfReads + lane.read2.numberOfReads + lane.umi
                  .map(_.numberOfReads)
                  .getOrElse(0L)
              case Some(_) =>
                val f = lane.umi.map(_ => 3).getOrElse(2)
                maxReadsPerChunk * f
            }

            val scratchNeeded = ((math.max(
              ResourceConfig.bwa.scratch.toDouble,
              ResourceConfig.uncompressedBamSizeBytePerRead.toDouble * (totalReads.toDouble))) / 1E6).toInt
            val resourceRequest =
              ResourceConfig.bwa.copy(
                cpuMemoryRequest = ResourceConfig.bwa.cpuMemoryRequest
                  .copy(scratch = scratchNeeded))

            log.info("BWA scratch space computed: " + scratchNeeded + "MB ")

            val newPartitionId =
              if (intervals.isDefined)
                PartitionId((lane.partition + 1) * 100000 + intervalIdx)
              else lane.partition

            alignSingleLane(
              PerLaneBWAAlignmentInput(lane.read1,
                                       lane.read2,
                                       project,
                                       sampleId,
                                       lane.runId,
                                       lane.lane,
                                       newPartitionId,
                                       reference,
                                       lane.umi,
                                       intervals))(resourceRequest)
          }

          for {
            splitIntervals <- computeManySplitIntervals(
              SplitFastQsInput(fastqs, maxReadsPerChunk))(
              ResourceConfig.minimal).map(_.intervals.toSeq)

            alignedLanes <- traverseAll(fastqs.toSeq)(fqPerLane =>
              alignLane(fqPerLane, splitIntervals.toMap)).map(_.flatten)

          } yield PerSampleBWAAlignmentResult(alignedLanes.toSet.toStable)
    }

  val sortByCoordinateAndIndex =
    AsyncTask[Bam, CoordinateSortedBam]("__sortbam", 1) {
      case Bam(bam) =>
        implicit computationEnvironment =>
          val tempFolder =
            TempFile
              .createTempFolder(".sortTempFolder")
              .getAbsolutePath
          val tmpSorted = Files.createTempFile(".bam")
          val tmpStdOut = Files.createTempFile(".stdout")
          val tmpStdErr = Files.createTempFile(".stderr")

          for {
            localBam <- bam.file
            result <- {

              Exec.bashAudit(
                logDiscriminator = "sortbam.sort",
                onError = Exec.ThrowIfNonZero)(s"""$samtoolsExecutable sort \\
                      -l 5 \\
                      -m ${(resourceAllocated.memory * 0.85 / resourceAllocated.cpu).toInt}M \\
                      -T ${tempFolder}/ \\
                      -\\@ ${resourceAllocated.cpu} \\
                      ${localBam.getAbsolutePath} \\
                  > ${tmpSorted.getAbsolutePath} 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
              """)

              Exec.bash("sortbam.idx", onError = Exec.ThrowIfNonZero)(
                s"""java ${JVM.serial} ${JVM.maxHeap} -Dpicard.useLegacyParser=false -jar $picardJar BuildBamIndex \\
                 --INPUT ${tmpSorted.getAbsolutePath} \\
                 > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)"""
              )

              val expectedBai =
                new File(
                  tmpSorted.getAbsolutePath
                    .stripSuffix(".bam") + ".bai")
              expectedBai.deleteOnExit

              val nameStub = bam.name.stripSuffix(".bam")

              Files.deleteRecursively(new File(tempFolder))

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
      3) {
      case BamsWithSampleMetadata(project, sampleId, bams) =>
        implicit computationEnvironment =>
          val tempFolder =
            TempFile
              .createTempFolder(".markDuplicateTempFolder")
              .getAbsolutePath

          val tmpDuplicateMarkedBam = Files.createTempFile(".bam")
          val tmpMetricsFile = Files.createTempFile(".metrics")
          val tmpStdOut = Files.createTempFile(".stdout")
          val tmpStdErr = Files.createTempFile(".stderr")

          val maxHeap = s"-Xmx${(resourceAllocated.memory * 0.8).toInt}m"
          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          for {
            localBams <- Future.traverse(bams.toSeq)(_.file.file)
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
          --OPTICAL_DUPLICATE_PIXEL_DISTANCE=2500 \\
          --CREATE_INDEX false \\
          --COMPRESSION_LEVEL 1 \\
          --MAX_RECORDS_IN_RAM 0 \\
          --SORTING_COLLECTION_SIZE_RATIO 0.125 \\
          --TMP_DIR $tempFolder \\
          > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)        
        """

              Exec.bashAudit(logDiscriminator = "markduplicates." + sampleId,
                             onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = project + "." + sampleId

              Files.deleteRecursively(new File(tempFolder))

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
                  BamWithSampleMetadata(project, sampleId, Bam(bam)),
                  DuplicationQCResult(duplicateMetric))

            }
          } yield result

    }

  /* Task in which bwa is executed
   *
   * Converts a pair (triple, if umi) of fastqs and metadata to an bam
   * Steps follow https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels
   *
   * Roughly:
   * 1. fastq to unmapped query sorted bam. copy umis into bam attribute if needed
   * 2. mark adapter, convert back to fastq, bwa, make bam. This second step is executed in
   *    a single pipe.
   */
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
                                    partition,
                                    reference,
                                    maybeUmi,
                                    maybeInterval) =>
        implicit computationEnvironment =>
          val bwaNumberOfThreads = math.max(1, resourceAllocated.cpu)

          val markAdapterMetricsFileOutput =
            Files.createTempFile(".markAdapterMetrics").getAbsolutePath

          val markAdapterTempFolder =
            TempFile.createTempFolder(".markAdapterTempFolder").getAbsolutePath

          val samToFastqTempFolder =
            TempFile.createTempFolder(".samToFastqTempFolder")

          val mergeBamAlignmentTempFolder =
            TempFile.createTempFolder(".mergeBamAlignmentTempFolder")

          val unmappedBamTempFolder =
            TempFile.createTempFolder(".unmappedBamFastqToSamTempFolder")

          val tmpCleanBam = Files.createTempFile(".bam")
          val tmpIntermediateUnmappedBam =
            Files.createTempFile(".unmappedBam")

          val tmpStdOut = Files.createTempFile(".stdout")
          val tmpStdErr = Files.createTempFile(".stderr")

          val readGroupName = project + "." + sampleId + "." + runId + "." + lane
          val uniqueSampleName = project + "." + sampleId

          val platformUnit = runId + "." + lane
          val sequencingCenter = "Health2030GenomeCenter"
          val runDate: String = java.time.Instant.now.toString

          val maxHeap = JVM.maxHeap
          val maxReads =
            (resourceAllocated.memory.toDouble * ResourceConfig.picardSamSortRecordPerMegabyteHeap).toLong

          val tmpDir =
            s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

          /* Extrats the interval from the fastq file
           *
           * Picard can't read a stream (it opens-closes the file at least twice), thus
           * we have to write it to disk
           */
          def fastqLoader(file: File,
                          interval: Option[VirtualPointerInterval]) =
            interval match {
              case None => (file.getAbsolutePath, false)
              case Some(VirtualPointerInterval(from, length)) =>
                import org.gc.fqsplit.FqSplitHelpers
                val buffer = Array.ofDim[Byte](8192)
                val tmp = Files.createTempFile("fq.gz")
                fileutils
                  .openFileOutputStream(tmp) { os =>
                    FqSplitHelpers.zipOutputStream(os) { os =>
                      FqSplitHelpers.readFastQSplit(file, from, length) { is =>
                        FqSplitHelpers.copy(is, os, buffer)
                      }
                    }
                  }
                (tmp.getAbsolutePath, true)
            }

          val resultF = for {
            read1 <- read1.file.file
            read2 <- read2.file.file
            umi <- maybeUmi match {
              case Some(umi) =>
                umi.file.file.map(Some(_))
              case None => Future.successful(None)
            }
            reference <- reference.localFile
            result <- {

              val (read1Fq, deleteRead1Fq) =
                fastqLoader(read1, maybeInterval.map(_.read1))
              val (read2Fq, deleteRead2Fq) =
                fastqLoader(read2, maybeInterval.map(_.read2))

              umi match {
                case None =>
                  val fastqToUnmappedBam = s"""\\
        java ${JVM.serial} $maxHeap $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
                --FASTQ $read1Fq \\
                --FASTQ2 $read2Fq \\
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

                  Exec.bashAudit(
                    logDiscriminator = "bwa.fastq2ubam." + sampleId,
                    onError = Exec.ThrowIfNonZero)(fastqToUnmappedBam)
                case Some(umi) =>
                  val (umiFq, deleteUmiFq) =
                    fastqLoader(umi, maybeInterval.flatMap(_.umi))
                  val fastqToUnmappedBam = s"""\\
        java ${JVM.serial} -Xmx2G $tmpDir -Dpicard.useLegacyParser=false -jar $picardJar FastqToSam \\
                --FASTQ $read1Fq \\
                --FASTQ2 $read2Fq \\
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
        java ${JVM.serial} -Xmx2G $tmpDir -jar $umiProcessorJar $umiFq \\
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

                  Exec.bashAudit(
                    logDiscriminator = "bwa.fastq2ubam_umi." + sampleId,
                    onError = Exec.ThrowIfNonZero)(fastqToUnmappedBam)

                  if (deleteUmiFq) {
                    new File(umiFq).delete
                  }
              }

              if (deleteRead1Fq) {
                new File(read1Fq).delete
              }
              if (deleteRead2Fq) {
                new File(read2Fq).delete
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

              Exec.bashAudit(logDiscriminator = "bwa.pipes." + sampleId,
                             onError = Exec.ThrowIfNonZero)(bashScript)

              log.info(
                s"Deleting intermediate unmapped bam $tmpIntermediateUnmappedBam .")
              tmpIntermediateUnmappedBam.delete

              Files.deleteRecursively(mergeBamAlignmentTempFolder)
              Files.deleteRecursively(unmappedBamTempFolder)
              Files.deleteRecursively(new File(markAdapterTempFolder))
              Files.deleteRecursively(samToFastqTempFolder)

              val nameStub = readGroupName + ".part" + partition

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
object ComputeSplitIntervalInput {
  implicit val encoder: Encoder[ComputeSplitIntervalInput] =
    deriveEncoder[ComputeSplitIntervalInput]
  implicit val decoder: Decoder[ComputeSplitIntervalInput] =
    deriveDecoder[ComputeSplitIntervalInput]
}
object SplitFastQsInput {
  implicit val encoder: Encoder[SplitFastQsInput] =
    deriveEncoder[SplitFastQsInput]
  implicit val decoder: Decoder[SplitFastQsInput] =
    deriveDecoder[SplitFastQsInput]
}
object SplitIntervalsResult {
  implicit val encoder: Encoder[SplitIntervalsResult] =
    deriveEncoder[SplitIntervalsResult]
  implicit val decoder: Decoder[SplitIntervalsResult] =
    deriveDecoder[SplitIntervalsResult]
}
object IntervalTriplet {
  implicit val encoder: Encoder[IntervalTriplet] =
    deriveEncoder[IntervalTriplet]
  implicit val decoder: Decoder[IntervalTriplet] =
    deriveDecoder[IntervalTriplet]
}

object PerSampleBWAAlignmentResult {
  implicit val encoder: Encoder[PerSampleBWAAlignmentResult] =
    deriveEncoder[PerSampleBWAAlignmentResult]
  implicit val decoder: Decoder[PerSampleBWAAlignmentResult] =
    deriveDecoder[PerSampleBWAAlignmentResult]
}
