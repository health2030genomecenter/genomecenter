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
import org.gc.pipelines.util
import org.gc.pipelines.util.{StableSet, traverseAll}
import org.gc.pipelines.util.StableSet.syntax

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
    umi: Option[FastQ]
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
case class SplitFastQsResult(intacts: StableSet[FastQPerLane],
                             splits: StableSet[FastQPerLane])
    extends WithSharedFiles(
      (intacts.toSeq ++ splits.toSeq)
        .flatMap(fq =>
          List(fq.read1.file, fq.read2.file) ++ fq.umi.toSeq.map(_.file)): _*
    ) {
  def splitFiles =
    splits.toSeq
      .flatMap(fq =>
        List(fq.read1.file, fq.read2.file) ++ fq.umi.toSeq.map(_.file))
}
case class SplitFastQInput(read1: FastQ,
                           read2: FastQ,
                           umi: Option[FastQ],
                           demultiplexingPartition: PartitionId,
                           maxReadsPerFastQ: Long)
    extends WithSharedFiles(
      read1.files ++ read2.files ++ umi.toSeq.flatMap(_.files): _*)

case class SplitFastQResult(
    split: Seq[(FastQ, FastQ, Option[FastQ], PartitionId)])
    extends WithSharedFiles(
      split.flatMap {
        case (read1, read2, umi, _) =>
          read1.files ++ read2.files ++ umi.toSeq.flatMap(_.files)
      }: _*
    )

case class PerSampleBWAAlignmentInput(
    fastqs: StableSet[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    reference: IndexedReferenceFasta,
    bamOfPreviousRuns: Option[Bam]
) extends WithSharedFiles(
      (fastqs
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq :+ reference.fasta) ++ bamOfPreviousRuns.toSeq
        .flatMap(_.files): _*)

case class DuplicationQCResult(markDuplicateMetrics: SharedFile)
    extends WithSharedFiles(markDuplicateMetrics)

case class MarkDuplicateResult(
    bam: BamWithSampleMetadata,
    duplicateMetric: DuplicationQCResult
) extends WithSharedFiles(bam.files ++ duplicateMetric.files: _*)

object BWAAlignment {

  val splitFastQTriple =
    AsyncTask[SplitFastQInput, SplitFastQResult]("__split-fastq", 1) {
      case SplitFastQInput(read1, read2, umi, demuxPartition, maxPerSplit) =>
        implicit computationEnvironment =>
          for {
            read1Local <- read1.file.file
            read2Local <- read2.file.file
            umiLocal <- umi match {
              case None      => Future.successful(None)
              case Some(umi) => umi.file.file.map(Some(_))
            }
            result <- {
              val partitionsRead1 =
                FastQHelpers.splitFastQ(read1Local, maxPerSplit)
              val partitionsRead2 =
                FastQHelpers.splitFastQ(read2Local, maxPerSplit)
              val partitionsUmi =
                umiLocal.map(f => FastQHelpers.splitFastQ(f, maxPerSplit))
              val read1And2 = partitionsRead1 zip partitionsRead2
              val withUmi = (partitionsUmi match {
                case Some(umi) => read1And2 zip umi.map(Some(_))
                case None      => read1And2.map(x => (x, None))
              }).zipWithIndex

              Future.traverse(withUmi) {
                case ((((split1, split1Reads), (split2, split2Reads)), mayUmi),
                      idx) =>
                  val newPartitionId =
                    PartitionId((demuxPartition + 1) * 10000 + idx)
                  for {
                    fq1 <- SharedFile(
                      split1,
                      read1.file.name + ".split." + newPartitionId + ".fq.gz",
                      deleteFile = true).map(FastQ(_, split1Reads))
                    fq2 <- SharedFile(
                      split2,
                      read2.file.name + ".split." + newPartitionId + ".fq.gz",
                      deleteFile = true).map(FastQ(_, split2Reads))
                    umi <- mayUmi match {
                      case None => Future.successful(None)
                      case Some((splitUmi, splitUmiReads)) =>
                        SharedFile(
                          splitUmi,
                          umi.get.file.name + ".split." + newPartitionId + ".fq.gz",
                          deleteFile = true).map(sf =>
                          Some(FastQ(sf, splitUmiReads)))
                    }
                  } yield (fq1, fq2, umi, newPartitionId)
              }
            }
          } yield SplitFastQResult(result)
    }

  val splitFastQs =
    AsyncTask[SplitFastQsInput, SplitFastQsResult]("__split-fastq", 1) {
      case SplitFastQsInput(fastqs, maxPerSplit) =>
        implicit computationEnvironment =>
          releaseResources
          for {
            fqPerLanesSplit <- traverseAll(fastqs.toSeq) { fastqPerLane =>
              if (fastqPerLane.read1.numberOfReads <= maxPerSplit) {
                Future.successful(Left(fastqPerLane))
              } else
                for {
                  split <- splitFastQTriple(
                    SplitFastQInput(read1 = fastqPerLane.read1,
                                    read2 = fastqPerLane.read2,
                                    umi = fastqPerLane.umi,
                                    demultiplexingPartition =
                                      fastqPerLane.partition,
                                    maxPerSplit))(ResourceConfig.minimal)
                } yield {
                  Right(split.split.map {
                    case (read1, read2, umi, partition) =>
                      FastQPerLane(fastqPerLane.runId,
                                   fastqPerLane.lane,
                                   read1,
                                   read2,
                                   umi,
                                   partition)
                  })

                }

            }
          } yield {
            val keptIntact = fqPerLanesSplit.collect {
              case Left(fqPerLane) => fqPerLane
            }
            val splits = fqPerLanesSplit.collect {
              case Right(splits) => splits
            }
            SplitFastQsResult(keptIntact.toSet.toStable,
                              splits.flatten.toSet.toStable)
          }
    }

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
    AsyncTask[PerSampleBWAAlignmentInput, MarkDuplicateResult](
      "__bwa-persample",
      1) {
      case PerSampleBWAAlignmentInput(fastqs,
                                      project,
                                      sampleId,
                                      reference,
                                      bamOfPreviousRuns) =>
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
                                       lane.runId,
                                       lane.lane,
                                       lane.partition,
                                       reference,
                                       lane.umi))(resourceRequest)
          }

          val maxReadsPerChunk = 5000000L

          for {
            splitFastQs <- splitFastQs(
              SplitFastQsInput(fastqs, maxReadsPerChunk))(
              ResourceConfig.minimal)
            alignedLanes <- traverseAll(
              splitFastQs.splits.toSeq ++ splitFastQs.intacts.toSeq)(alignLane)
            merged <- mergeAndMarkDuplicate(
              BamsWithSampleMetadata(
                project,
                sampleId,
                StableSet(
                  alignedLanes.map(_.bam) ++ bamOfPreviousRuns.toSet: _*)))(
              ResourceConfig.picardMergeAndMarkDuplicates)
            _ <- Future.traverse(alignedLanes.map(_.bam))(_.file.delete)
            _ <- Future.traverse(splitFastQs.splitFiles)((_: SharedFile).delete)

          } yield merged
    }

  val sortByCoordinateAndIndex =
    AsyncTask[Bam, CoordinateSortedBam]("__sortbam", 1) {
      case Bam(bam) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()
          val samtoolsExecutable = extractSamtoolsExecutable()
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

              Exec.bash(
                logDiscriminator = "sortbam.sort",
                onError = Exec.ThrowIfNonZero)(s"""$samtoolsExecutable sort \\
                      -l 5 \\
                      -m ${(resourceAllocated.memory * 0.95 / resourceAllocated.cpu).toInt}M \\
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
      2) {
      case BamsWithSampleMetadata(project, sampleId, bams) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

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

              Exec.bash(logDiscriminator = "markduplicates." + sampleId,
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
                                    maybeUmi) =>
        implicit computationEnvironment =>
          val picardJar = extractPicardJar()

          val bwaExecutable = extractBwaExecutable()

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

  def extractFakeRscript(): File =
    fileutils.TempFile
      .getExecutableFromJar("/bin/Rscript", "Rscript")

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
  private def extractSamtoolsExecutable(): String = {
    val resourceName =
      if (util.isMac) "/bin/samtools_1.9_mac"
      else if (util.isLinux) "/bin/samtools_1.9_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))

    fileutils.TempFile
      .getExecutableFromJar(resourceName, "samtools_1.9")
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
object SplitFastQInput {
  implicit val encoder: Encoder[SplitFastQInput] =
    deriveEncoder[SplitFastQInput]
  implicit val decoder: Decoder[SplitFastQInput] =
    deriveDecoder[SplitFastQInput]
}
object SplitFastQsInput {
  implicit val encoder: Encoder[SplitFastQsInput] =
    deriveEncoder[SplitFastQsInput]
  implicit val decoder: Decoder[SplitFastQsInput] =
    deriveDecoder[SplitFastQsInput]
}
object SplitFastQResult {
  implicit val encoder: Encoder[SplitFastQResult] =
    deriveEncoder[SplitFastQResult]
  implicit val decoder: Decoder[SplitFastQResult] =
    deriveDecoder[SplitFastQResult]
}
object SplitFastQsResult {
  implicit val encoder: Encoder[SplitFastQsResult] =
    deriveEncoder[SplitFastQsResult]
  implicit val decoder: Decoder[SplitFastQsResult] =
    deriveDecoder[SplitFastQsResult]
}
