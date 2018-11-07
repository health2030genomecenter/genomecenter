package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import fileutils.TempFile
import org.gc.pipelines.util.{Exec, GATK, JVM, ResourceConfig, Files}
import java.io.File

case class HaplotypeCallerInput(
    bam: CoordinateSortedBam,
    indexedReference: IndexedReferenceFasta
) extends WithSharedFiles(bam.files ++ indexedReference.files: _*)

case class VariantCallingMetricsResult(
    details: SharedFile,
    summary: SharedFile
) extends WithSharedFiles(details, summary)

case class MergeVCFInput(vcfs: Seq[VCF], gatheredFileName: String)
    extends WithSharedFiles(
      vcfs.flatMap(_.files): _*
    )

case class HaplotypeCallerOnIntervalInput(
    bam: CoordinateSortedBam,
    indexedReference: IndexedReferenceFasta,
    interval: String
) extends WithSharedFiles(bam.files ++ indexedReference.files: _*)

case class CollectVariantCallingMetricsInput(
    reference: IndexedReferenceFasta,
    targetVcf: VCF,
    dbSnpVcf: VCF,
    evaluationIntervals: BedFile
) extends WithSharedFiles(
      reference.files ++ targetVcf.files ++ dbSnpVcf.files ++ evaluationIntervals.files: _*)

case class GenotypeGVCFsOnIntervalInput(targetVcfs: Set[VCF],
                                        reference: IndexedReferenceFasta,
                                        dbSnpVcf: VCF,
                                        interval: String,
                                        name: String)
    extends WithSharedFiles(
      targetVcfs.toSeq
        .flatMap(_.files) ++ reference.files ++ dbSnpVcf.files: _*)

case class GenotypeGVCFsInput(targetVcfs: Set[VCF],
                              reference: IndexedReferenceFasta,
                              dbSnpVcf: VCF,
                              name: String)
    extends WithSharedFiles(
      targetVcfs.toSeq
        .flatMap(_.files) ++ reference.files ++ dbSnpVcf.files: _*)

case class GenotypesOnInterval(genotypes: VCF, sites: VCF)
    extends WithSharedFiles(genotypes.files ++ sites.files: _*)

case class GenotypeGVCFsResult(sites: VCF, genotypesScattered: Seq[VCF])
    extends WithSharedFiles(
      sites.files ++ genotypesScattered.flatMap(_.files): _*)

object HaplotypeCaller {

  val genotypeGvcfs =
    AsyncTask[GenotypeGVCFsInput, GenotypeGVCFsResult]("__genotypegvcfs", 1) {
      case GenotypeGVCFsInput(vcfs, reference, dbsnp, name) =>
        implicit computationEnvironment =>
          releaseResources

          def intoScattersFolder[T] =
            appendToFilePrefix[T](Seq("scatters"))

          for {
            dict <- reference.dict
            intervals = BaseQualityScoreRecalibration
              .createIntervals(dict)
              .filterNot(_ == "unmapped")
            scattered <- Future.traverse(intervals) { interval =>
              intoScattersFolder { implicit computationEnvironment =>
                genotypeGvcfsOnInterval(
                  GenotypeGVCFsOnIntervalInput(vcfs,
                                               reference,
                                               dbsnp,
                                               interval,
                                               name))(
                  ResourceConfig.genotypeGvcfs)
              }
            }
            scatteredGenotypes = scattered.map(_.genotypes)
            scatteredSites = scattered.map(_.sites)
            gatheredSites <- mergeVcfs(
              MergeVCFInput(scatteredSites, name + ".sites_only.vcf.gz"))(
              ResourceConfig.minimal)
            _ <- Future.traverse(scatteredSites)(_.vcf.delete)
          } yield GenotypeGVCFsResult(gatheredSites, scatteredGenotypes)
    }

  val genotypeGvcfsOnInterval =
    AsyncTask[GenotypeGVCFsOnIntervalInput, GenotypesOnInterval](
      "__genotypegvcf-interval",
      1) {
      case GenotypeGVCFsOnIntervalInput(vcfs,
                                        reference,
                                        dbsnp,
                                        interval,
                                        name) =>
        implicit computationEnvironment =>
          for {
            localVcfs <- Future.traverse(vcfs)(_.localFile)
            reference <- reference.localFile
            dbsnp <- dbsnp.localFile
            vcf <- {
              val gatkJar = BaseQualityScoreRecalibration.extractGatkJar()
              val picardJar = BWAAlignment.extractPicardJar()

              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val vcfOutput = TempFile.createTempFile(".vcf.gz")
              val vcfOutputFiltered =
                TempFile.createTempFile(".vcf.gz").getAbsolutePath
              val vcfOutputFilteredSitesOnly =
                TempFile.createTempFile(".vcf.gz").getAbsolutePath

              val genomeDbWorkfolder = TempFile.createTempFile(".gdb")
              genomeDbWorkfolder.delete

              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              {
                val input = " -V " + localVcfs
                  .map(_.getAbsolutePath)
                  .mkString(" -V ")

                Exec.bash(logDiscriminator = "genomedbimport",
                          onError = Exec.ThrowIfNonZero)(s"""\\
        java -Xmx4G ${JVM.serial} \\
          ${GATK.javaArguments(5)} \\
          -jar $gatkJar GenomicsDBImport \\
            --genomicsdb-workspace-path ${genomeDbWorkfolder.getAbsolutePath} \\
            --intervals $interval \\
            --batch-size 50 --reader-threads 5 -ip 500 \\
            $input \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)
              }

              Exec.bash(logDiscriminator = "genotypegvcfs",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java -Xmx5G ${JVM.serial} \\
          ${GATK.javaArguments(compressionLevel = 1)} \\
          -jar $gatkJar GenotypeGVCFs \\
            --reference ${reference.getAbsolutePath} \\
            --output ${vcfOutput.getAbsolutePath} \\
            -D ${dbsnp.getAbsolutePath} \\
            -G StandardAnnotation \\
            --only-output-calls-starting-in-intervals \\
            --use-new-qual-calculator \\
            --intervals $interval \\
            -V gendb://${genomeDbWorkfolder.getAbsolutePath} \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              /* For the 54.69 magic number refer to
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/5585cdf7877104f2c61b2720ddfe7235f2fad577/JointGenotypingWf.wdl#L58
               */
              Exec.bash(logDiscriminator = "variantfilter",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java -Xmx5G ${JVM.serial} \\
          ${GATK.javaArguments(compressionLevel = 5)} \\
          -jar $gatkJar VariantFiltration \\
            --filter-expression "ExcessHet > 54.69" \\
            --filter-name ExcessHet \\
            --output $vcfOutputFiltered \\
            -V ${vcfOutput.getAbsolutePath} \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              Exec.bash(logDiscriminator = "removegenotypes",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false \\
          -jar $picardJar MakeSitesOnlyVcf \\
            --INPUT $vcfOutputFiltered \\
            --OUTPUT $vcfOutputFilteredSitesOnly \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              Files.deleteRecursively(genomeDbWorkfolder)
              vcfOutput.delete

              val nameStub = name + "." + interval + ".vcf.gz"

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                vcf <- SharedFile(new File(vcfOutputFiltered), nameStub, true)
                vcfIndex <- SharedFile(new File(vcfOutputFiltered + ".tbi"),
                                       nameStub + ".tbi",
                                       true)
                sitesOnlyVcf <- SharedFile(
                  new File(vcfOutputFilteredSitesOnly),
                  nameStub.stripSuffix("vcf.gz") + "sites_only.vcf.gz",
                  true)
                sitesOnlyVcfIdx <- SharedFile(
                  new File(vcfOutputFilteredSitesOnly + ".tbi"),
                  nameStub.stripSuffix("vcf.gz") + "sites_only.vcf.gz.tbi",
                  true)

              } yield
                GenotypesOnInterval(genotypes = VCF(vcf, Some(vcfIndex)),
                                    sites =
                                      VCF(sitesOnlyVcf, Some(sitesOnlyVcfIdx)))
            }

          } yield vcf

    }

  val haplotypeCaller =
    AsyncTask[HaplotypeCallerInput, VCF]("__haplotypecaller", 1) {
      case HaplotypeCallerInput(bam, reference) =>
        implicit computationEnvironment =>
          releaseResources

          def intoScattersFolder[T] =
            appendToFilePrefix[T](Seq("scatters"))

          for {
            dict <- reference.dict
            intervals = BaseQualityScoreRecalibration
              .createIntervals(dict)
              .filterNot(_ == "unmapped")
            scattered <- Future.traverse(intervals) { interval =>
              intoScattersFolder { implicit computationEnvironment =>
                haplotypeCallerOnInterval(
                  HaplotypeCallerOnIntervalInput(bam, reference, interval))(
                  ResourceConfig.haplotypeCaller)
              }
            }
            gathered <- mergeVcfs(
              MergeVCFInput(scattered, bam.bam.name + ".hc.gvcf.vcf.gz"))(
              ResourceConfig.minimal)
            _ <- Future.traverse(scattered)(_.vcf.delete)
          } yield gathered
    }

  val mergeVcfs = AsyncTask[MergeVCFInput, VCF]("__mergevcfs", 1) {
    case MergeVCFInput(vcfs, gatheredFileName) =>
      implicit computationEnvironment =>
        for {
          localVcfs <- Future.traverse(vcfs)(_.vcf.file)
          merged <- {

            val picardJar = BWAAlignment.extractPicardJar()
            val tmpStdOut = TempFile.createTempFile(".stdout")
            val tmpStdErr = TempFile.createTempFile(".stderr")
            val vcfOutput = TempFile.createTempFile(".vcf.gz").getAbsolutePath

            val javaTmpDir =
              s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

            val input = " --INPUT " + localVcfs
              .map(_.getAbsolutePath)
              .mkString(" --INPUT ")

            Exec.bash(logDiscriminator = "gathervcf",
                      onError = Exec.ThrowIfNonZero)(s""" \\
                  java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar MergeVcfs \\
                  $input \\
                  --OUTPUT $vcfOutput \\
                        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                        """)

            val expectedIndex = new File(vcfOutput + ".tbi")

            for {
              _ <- SharedFile(tmpStdOut, gatheredFileName + ".stdout", true)
              _ <- SharedFile(tmpStdErr, gatheredFileName + ".stderr", true)
              vcf <- SharedFile(new File(vcfOutput), gatheredFileName, true)
              vcfIndex <- SharedFile(expectedIndex,
                                     gatheredFileName + ".tbi",
                                     true)

            } yield VCF(vcf, Some(vcfIndex))

          }
        } yield merged
  }

  val haplotypeCallerOnInterval =
    AsyncTask[HaplotypeCallerOnIntervalInput, VCF]("__haplotypecaller-interval",
                                                   1) {
      case HaplotypeCallerOnIntervalInput(bam, reference, interval) =>
        implicit computationEnvironment =>
          for {
            localBam <- bam.localFile
            localReference <- reference.localFile
            vcf <- {
              val gatkJar = BaseQualityScoreRecalibration.extractGatkJar()

              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val vcfOutput = TempFile.createTempFile(".vcf.gz").getAbsolutePath

              val maxHeap = s"-Xmx${resourceAllocated.memory}m"

              val bashScript = s"""\\
         java $maxHeap ${JVM.g1} ${GATK.javaArguments(compressionLevel = 5)} \\
      -jar $gatkJar HaplotypeCaller \\
      --reference ${localReference.getAbsolutePath} \\
      --output $vcfOutput \\
      --input ${localBam.getAbsolutePath} \\
      --intervals $interval \\
      --emit-ref-confidence GVCF \\
      --max-alternate-alleles 3 \\
      --read-filter OverclippedReadFilter \\
      > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """

              Exec.bash(logDiscriminator = "haplotypecaller",
                        onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = bam.bam.name + "." + interval + ".hc.gvcf.vcf.gz"

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout")
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr")
                vcf <- SharedFile(new File(vcfOutput), nameStub)
                vcfIndex <- SharedFile(new File(vcfOutput + ".tbi"),
                                       nameStub + ".tbi")

              } yield VCF(vcf, Some(vcfIndex))
            }

          } yield vcf
    }

  val collectVariantCallingMetrics =
    AsyncTask[CollectVariantCallingMetricsInput, VariantCallingMetricsResult](
      "__collectvariantcallingmetrics",
      1) {
      case CollectVariantCallingMetricsInput(
          reference,
          targetVcf,
          dbSnpVcf,
          BedFile(evaluationIntervals)
          ) =>
        implicit computationEnvironment =>
          for {
            refDict <- reference.dict
            reference <- reference.localFile
            localTargetVcf <- targetVcf.localFile
            dbSnpVcf <- dbSnpVcf.localFile
            evaluationIntervals <- evaluationIntervals.file
            result <- {

              val picardJar = BWAAlignment.extractPicardJar()
              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val tmpOut = TempFile.createTempFile(".metrics").getAbsolutePath
              val picardStyleInterval = TempFile.createTempFile("")

              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              Exec.bash(logDiscriminator = "collectvariantcallqc",
                        onError = Exec.ThrowIfNonZero)(s""" \\
        java ${JVM.serial} -Xmx2G -Dpicard.useLegacyParser=false -jar $picardJar BedToIntervalList \\
          --INPUT ${evaluationIntervals.getAbsolutePath} \\
          --SEQUENCE_DICTIONARY ${refDict.getAbsolutePath} \\
          --OUTPUT ${picardStyleInterval.getAbsolutePath} \\
        """)

              Exec.bash(logDiscriminator = "collectvariantcallqc",
                        onError = Exec.ThrowIfNonZero)(s""" \\
                   java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar CollectVariantCallingMetrics \\
                    --INPUT ${localTargetVcf.getAbsolutePath} \\
                    --OUTPUT $tmpOut \\
                    --DBSNP ${dbSnpVcf.getAbsolutePath} \\
                    --SEQUENCE_DICTIONARY ${refDict.getAbsolutePath} \\
                    --TARGET_INTERVALS ${picardStyleInterval.getAbsolutePath} \\
                    --GVCF_INPUT true \\
                   > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                        """)

              val expectedDetails =
                new File(tmpOut + ".variant_calling_detail_metrics")
              val expectedSummary =
                new File(tmpOut + ".variant_calling_summary_metrics")
              val nameStub = targetVcf.vcf.name + ".qc"
              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout")
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr")
                detail <- SharedFile(
                  expectedDetails,
                  nameStub + ".variant_calling_detail_metrics")
                summary <- SharedFile(
                  expectedSummary,
                  nameStub + ".variant_calling_summary_metrics")

              } yield VariantCallingMetricsResult(detail, summary)

            }
          } yield result

    }

}

object HaplotypeCallerInput {
  implicit val encoder: Encoder[HaplotypeCallerInput] =
    deriveEncoder[HaplotypeCallerInput]
  implicit val decoder: Decoder[HaplotypeCallerInput] =
    deriveDecoder[HaplotypeCallerInput]
}

object MergeVCFInput {
  implicit val encoder: Encoder[MergeVCFInput] =
    deriveEncoder[MergeVCFInput]
  implicit val decoder: Decoder[MergeVCFInput] =
    deriveDecoder[MergeVCFInput]
}

object HaplotypeCallerOnIntervalInput {
  implicit val encoder: Encoder[HaplotypeCallerOnIntervalInput] =
    deriveEncoder[HaplotypeCallerOnIntervalInput]
  implicit val decoder: Decoder[HaplotypeCallerOnIntervalInput] =
    deriveDecoder[HaplotypeCallerOnIntervalInput]
}

object VariantCallingMetricsResult {
  implicit val encoder: Encoder[VariantCallingMetricsResult] =
    deriveEncoder[VariantCallingMetricsResult]
  implicit val decoder: Decoder[VariantCallingMetricsResult] =
    deriveDecoder[VariantCallingMetricsResult]
}

object CollectVariantCallingMetricsInput {
  implicit val encoder: Encoder[CollectVariantCallingMetricsInput] =
    deriveEncoder[CollectVariantCallingMetricsInput]
  implicit val decoder: Decoder[CollectVariantCallingMetricsInput] =
    deriveDecoder[CollectVariantCallingMetricsInput]
}

object GenotypeGVCFsOnIntervalInput {
  implicit val encoder: Encoder[GenotypeGVCFsOnIntervalInput] =
    deriveEncoder[GenotypeGVCFsOnIntervalInput]
  implicit val decoder: Decoder[GenotypeGVCFsOnIntervalInput] =
    deriveDecoder[GenotypeGVCFsOnIntervalInput]
}

object GenotypeGVCFsInput {
  implicit val encoder: Encoder[GenotypeGVCFsInput] =
    deriveEncoder[GenotypeGVCFsInput]
  implicit val decoder: Decoder[GenotypeGVCFsInput] =
    deriveDecoder[GenotypeGVCFsInput]
}

object GenotypeGVCFsResult {
  implicit val encoder: Encoder[GenotypeGVCFsResult] =
    deriveEncoder[GenotypeGVCFsResult]
  implicit val decoder: Decoder[GenotypeGVCFsResult] =
    deriveDecoder[GenotypeGVCFsResult]
}

object GenotypesOnInterval {
  implicit val encoder: Encoder[GenotypesOnInterval] =
    deriveEncoder[GenotypesOnInterval]
  implicit val decoder: Decoder[GenotypesOnInterval] =
    deriveDecoder[GenotypesOnInterval]
}
