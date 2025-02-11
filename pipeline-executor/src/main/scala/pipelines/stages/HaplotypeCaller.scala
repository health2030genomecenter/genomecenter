package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.{Future, ExecutionContext}
import org.gc.pipelines.model.{SampleId, Project}
import org.gc.pipelines.util.{
  Exec,
  GATK,
  JVM,
  ResourceConfig,
  Files,
  StableSet,
  Fasta,
  traverseAll
}
import StableSet.syntax
import java.io.File
import scala.collection.JavaConverters._
import Executables.{picardJar, gatkJar, bcftoolsExecutable}

case class HaplotypeCallerInput(
    bam: CoordinateSortedBam,
    indexedReference: IndexedReferenceFasta,
    contigs: Option[ContigsFile],
)

case class VariantCallingMetricsResult(
    details: SharedFile,
    summary: SharedFile
)

case class MergeVCFInput(vcfs: Seq[VCF], gatheredFileName: String)

case class HaplotypeCallerOnIntervalInput(
    bam: CoordinateSortedBam,
    indexedReference: IndexedReferenceFasta,
    interval: String
)

case class CollectVariantCallingMetricsInput(
    reference: IndexedReferenceFasta,
    targetVcf: VCF,
    dbSnpVcf: VCF,
    evaluationIntervals: Option[BedFile]
)

case class GenotypeGVCFsOnIntervalInput(targetVcfs: StableSet[VCF],
                                        reference: IndexedReferenceFasta,
                                        dbSnpVcf: VCF,
                                        interval: String,
                                        name: String,
                                        atSitesOnly: Option[StableSet[VCF]])

case class GenotypeGVCFsInput(targetVcfs: StableSet[VCF],
                              reference: IndexedReferenceFasta,
                              dbSnpVcf: VCF,
                              name: String,
                              vqsrTrainingFiles: Option[VQSRTrainingFiles],
                              contigsFile: Option[ContigsFile],
                              atSitesOnly: Option[StableSet[VCF]])

case class GenotypesOnInterval(genotypes: VCF, sites: VCF)

case class GenotypeGVCFsResult(sites: VCF, genotypesScattered: Seq[VCF])

case class TrainSnpVQSRInput(
    vcf: VCF,
    hapmap: VCF,
    omni: VCF,
    oneKg: VCF,
    knownSites: VCF,
)

case class TrainIndelVQSRInput(
    vcf: VCF,
    millsAnd1Kg: VCF,
    knownSites: VCF,
)

case class TrainVQSRResult(
    recal: SharedFile,
    recalIdx: SharedFile,
    tranches: SharedFile
) {
  def localRecalFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- recalIdx.file
      recal <- recal.file
    } yield recal
}

case class ApplyVQSRInput(
    vcf: VCF,
    snp: TrainVQSRResult,
    indel: TrainVQSRResult
)

case class JointCallInput(
    haplotypeCallerReferenceCalls: StableSet[VCF],
    indexedReference: IndexedReferenceFasta,
    dbSnpVcf: VCF,
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    outputFileName: String,
    contigsFile: Option[ContigsFile]
)
case class MergeSingleCallsInput(
    haplotypeCallerReferenceCalls: StableSet[VCF],
    nonRefSites: StableSet[VCF],
    indexedReference: IndexedReferenceFasta,
    dbSnpVcf: VCF,
    outputFileName: String,
    contigsFile: Option[ContigsFile]
)

case class SingleSampleVariantDiscoveryInput(
    bam: CoordinateSortedBam,
    indexedReference: IndexedReferenceFasta,
    contigsFile: Option[ContigsFile],
    dbSnpVcf: VCF,
    project: Project,
    sampleId: SampleId,
    variantEvaluationIntervals: BedFile,
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    keepVcf: Boolean)

case class SingleSampleVariantDiscoveryResult(
    haplotypeCallerReferenceCalls: Option[VCF],
    genotypedVcf: Option[VCF],
    gvcfQCInterval: VariantCallingMetricsResult,
    gvcfQCOverall: VariantCallingMetricsResult)
    extends WithSharedFiles(
      mutables = haplotypeCallerReferenceCalls.toSeq ++ genotypedVcf.toSeq ++ List(
        gvcfQCInterval,
        gvcfQCOverall)
    )

case class CombineGenotypesInput(vcfs: StableSet[VCF], outputFileName: String)

object HaplotypeCaller {

  val singleSampleVariantDiscovery =
    AsyncTask[SingleSampleVariantDiscoveryInput,
              SingleSampleVariantDiscoveryResult](
      "__single-sample-variant-discovery",
      1) {
      case SingleSampleVariantDiscoveryInput(bam,
                                             indexedReference,
                                             contigsFile,
                                             dbSnpVcf,
                                             project,
                                             sampleId,
                                             variantEvaluationIntervals,
                                             vqsrTrainingFiles,
                                             keepVcf) =>
        implicit computationEnvironment =>
          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](Seq("intermediate"))
          def intoQCFolder[T] =
            appendToFilePrefix[T](Seq("QC"))

          releaseResources

          for {
            haplotypeCallerReferenceCalls <- HaplotypeCaller.haplotypeCaller(
              HaplotypeCallerInput(bam, indexedReference, contigsFile))(
              ResourceConfig.minimal)

            GenotypeGVCFsResult(_, genotypesScattered) <- intoIntermediateFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.genotypeGvcfs(
                  GenotypeGVCFsInput(
                    StableSet(haplotypeCallerReferenceCalls),
                    indexedReference,
                    dbSnpVcf,
                    project + "." + sampleId + ".single",
                    vqsrTrainingFiles = vqsrTrainingFiles,
                    contigsFile = contigsFile,
                    atSitesOnly = None
                  ))(ResourceConfig.minimal)
            }

            genotypedVcf <- HaplotypeCaller.mergeVcfs(
              MergeVCFInput(
                genotypesScattered,
                project + "." + sampleId + ".single.genotyped.vcf.gz"))(
              ResourceConfig.minimal
            )

            startGvcfQCInInterval = intoQCFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.collectVariantCallingMetrics(
                  CollectVariantCallingMetricsInput(
                    indexedReference,
                    genotypedVcf,
                    dbSnpVcf,
                    Some(variantEvaluationIntervals)))(
                  ResourceConfig.minimal
                )
            }
            startGvcfQCOverall = intoQCFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.collectVariantCallingMetrics(
                  CollectVariantCallingMetricsInput(indexedReference,
                                                    genotypedVcf,
                                                    dbSnpVcf,
                                                    None))(
                  ResourceConfig.minimal)
            }
            gvcfQCInterval <- startGvcfQCInInterval
            gvcfQCOverall <- startGvcfQCOverall

            _ <- if (!keepVcf) haplotypeCallerReferenceCalls.vcf.delete
            else Future.successful(())
            _ <- if (!keepVcf) genotypedVcf.vcf.delete
            else Future.successful(())
          } yield
            SingleSampleVariantDiscoveryResult(
              if (keepVcf) Some(haplotypeCallerReferenceCalls) else None,
              if (keepVcf) Some(genotypedVcf) else None,
              gvcfQCInterval,
              gvcfQCOverall)

    }

  lazy val defaultContigs: Set[String] =
    scala.io.Source
      .fromInputStream(
        getClass.getResourceAsStream("/variantcallingcontigs.txt"))
      .getLines
      .toSet

  val jointCall =
    AsyncTask[JointCallInput, VCF]("__joint-call", 1) {
      case JointCallInput(haplotypeCallerReferenceCalls,
                          indexedReference,
                          dbSnpVcf,
                          vqsrTrainingFiles,
                          outputFileName,
                          contigsFile) =>
        implicit computationEnvironment =>
          releaseResources

          val samples = haplotypeCallerReferenceCalls.size + "h" + haplotypeCallerReferenceCalls.hashCode

          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](Seq("intermediate").filter(_.nonEmpty))

          for {
            GenotypeGVCFsResult(_, genotypesScattered) <- intoIntermediateFolder {
              implicit computationEnvironment =>
                HaplotypeCaller.genotypeGvcfs(
                  GenotypeGVCFsInput(
                    haplotypeCallerReferenceCalls,
                    indexedReference,
                    dbSnpVcf,
                    outputFileName + ".genotypes",
                    vqsrTrainingFiles = vqsrTrainingFiles,
                    contigsFile = contigsFile,
                    atSitesOnly = None
                  ))(ResourceConfig.minimal)
            }

            genotypesVCF <- HaplotypeCaller.mergeVcfs(
              MergeVCFInput(
                genotypesScattered,
                outputFileName + s".joint.$samples.genotyped.vcf.gz"))(
              ResourceConfig.minimal)

          } yield genotypesVCF

    }

  val mergeSingleCalls =
    AsyncTask[MergeSingleCallsInput, VCF]("__joint-call", 1) {
      case MergeSingleCallsInput(haplotypeCallerReferenceCalls,
                                 singleCallNonRefSites,
                                 indexedReference,
                                 dbSnpVcf,
                                 outputFileName,
                                 contigsFile) =>
        implicit computationEnvironment =>
          releaseResources

          val samples = haplotypeCallerReferenceCalls.size + "h" + haplotypeCallerReferenceCalls.hashCode

          def intoIntermediateFolder[T] =
            appendToFilePrefix[T](Seq("intermediate").filter(_.nonEmpty))

          for {
            genotypedVCFS <- intoIntermediateFolder {
              implicit computationEnvironment =>
                traverseAll(haplotypeCallerReferenceCalls.toSeq) {
                  haplotypeCallerReferenceCall =>
                    for {
                      GenotypeGVCFsResult(_, genotypesScattered) <- HaplotypeCaller
                        .genotypeGvcfs(
                          GenotypeGVCFsInput(
                            StableSet(haplotypeCallerReferenceCall),
                            indexedReference,
                            dbSnpVcf,
                            haplotypeCallerReferenceCall.vcf.name + ".genotypesAtUnion",
                            vqsrTrainingFiles = None,
                            contigsFile = contigsFile,
                            atSitesOnly = Some(singleCallNonRefSites)
                          ))(ResourceConfig.minimal)

                      genotypedVCF <- HaplotypeCaller.mergeVcfs(MergeVCFInput(
                        genotypesScattered,
                        haplotypeCallerReferenceCall.vcf.name + s".genotypesAtUnion.genotyped.vcf.gz"))(
                        ResourceConfig.minimal)
                    } yield genotypedVCF
                }
            }

            combined <- HaplotypeCaller.combineGenotypes(
              CombineGenotypesInput(
                genotypedVCFS.toSet.toStable,
                outputFileName + ".merged." + samples + ".vcf.gz"))(
              ResourceConfig.minimal)

          } yield combined

    }

  val combineGenotypes =
    AsyncTask[CombineGenotypesInput, VCF]("__combine-genotypes", 1) {
      case CombineGenotypesInput(vcfs, outputFileName) =>
        implicit computationEnvironment =>
          for {
            localVcfs <- Future.traverse(vcfs.toSeq)(_.vcf.file)
            merged <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val vcfOutput = Files.createTempFile(".vcf.gz").getAbsolutePath

              val input = localVcfs
                .map(_.getAbsolutePath)
                .mkString(" ")

              Exec.bashAudit(logDiscriminator = "bcftools.merge",
                             onError = Exec.ThrowIfNonZero)(s""" \\
              $bcftoolsExecutable merge --output $vcfOutput --output-type z $input \\
                    > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                    """)

              for {
                _ <- SharedFile(tmpStdOut, outputFileName + ".stdout", true)
                _ <- SharedFile(tmpStdErr, outputFileName + ".stderr", true)
                vcf <- SharedFile(new File(vcfOutput), outputFileName, true)
              } yield VCF(vcf, None)

            }
          } yield merged

    }

  val applyVQSR =
    AsyncTask[ApplyVQSRInput, VCF]("__apply-vqsr", 1) {
      case ApplyVQSRInput(vcf, snpTrain, indelTrain) =>
        implicit computationEnvironment =>
          for {
            localVCF <- vcf.localFile
            snpRecal <- snpTrain.localRecalFile
            snpTranches <- snpTrain.tranches.file
            indelRecal <- indelTrain.localRecalFile
            indelTranches <- indelTrain.tranches.file
            result <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val tmpVcf = Files.createTempFile(".tmp.vcf.gz")
              val finalVcf = Files.createTempFile(".vcf.gz")
              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              /*
               * 99.7 magic number is from https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               */
              Exec.bash(logDiscriminator = "vqsr-apply-indel",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java ${JVM.serial} $javaTmpDir ${JVM.maxHeap} \\
          ${GATK.javaArguments(5)} \\
            -jar $gatkJar ApplyVQSR \\
              -V ${localVCF.getAbsolutePath} \\
              -O ${tmpVcf.getAbsolutePath} \\
              --tranches-file ${indelTranches.getAbsolutePath} \\
              --recal-file ${indelRecal.getAbsolutePath} \\
              --create-output-variant-index true \\
              -mode INDEL \\
              ${GATK.skipGcs} \\
              --truth-sensitivity-filter-level 99.7
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              Exec.bash(logDiscriminator = "vqsr-apply-snp",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java ${JVM.serial} $javaTmpDir ${JVM.maxHeap} \\
          ${GATK.javaArguments(5)} \\
            -jar $gatkJar ApplyVQSR \\
              -V ${tmpVcf.getAbsolutePath} \\
              -O ${finalVcf.getAbsolutePath} \\
              --tranches-file ${snpTranches.getAbsolutePath} \\
              --recal-file ${snpRecal.getAbsolutePath} \\
              --create-output-variant-index true \\
              -mode SNP \\
              ${GATK.skipGcs} \\
              --truth-sensitivity-filter-level 99.7
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = vcf.vcf.name.stripSuffix(".vcf.gz") + ".vqsr"
              tmpVcf.delete

              val expectVcfIndex = new File(finalVcf.getAbsolutePath + ".tbi")
              expectVcfIndex.deleteOnExit

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                vcf <- SharedFile(finalVcf, nameStub + ".vcf.gz", true)
                vcfidx <- SharedFile(expectVcfIndex,
                                     nameStub + ".vcf.gz.tbi",
                                     true)

              } yield VCF(vcf, Some(vcfidx))

            }
          } yield result

    }

  val trainSnpVQSR =
    AsyncTask[TrainSnpVQSRInput, TrainVQSRResult]("__train-vqsr-snp", 1) {
      case input =>
        implicit computationEnvironment =>
          for {
            localVcf <- input.vcf.localFile
            hapmap <- input.hapmap.localFile
            omni <- input.omni.localFile
            oneKg <- input.oneKg.localFile
            knownSites <- input.knownSites.localFile
            result <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val recalOutput = Files.createTempFile(".recal")
              val trancheOutput = Files.createTempFile(".tranches")
              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              /* values are from:
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               */
              val tranches =
                List(100.0, 99.95, 99.9, 99.8, 99.6, 99.5, 99.4, 99.3, 99.0,
                  98.0, 97.0, 90.0).mkString(" -tranche ", " -tranche ", "")

              /* values are from:
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               *
               *  I removed DP annotation as some suggest
               *  that it is not appropriate for exomes due to high variation
               *  in coverage due to capture
               */
              val annotations =
                List("QD", "MQRankSum", "ReadPosRankSum", "FS", "MQ", "SOR")
                  .mkString(" -an ", " -an ", "")

              Exec.bash(logDiscriminator = "vqsr-train-snp",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java ${JVM.serial} $javaTmpDir ${JVM.maxHeap} \\
          ${GATK.javaArguments(5)} \\
            -jar $gatkJar VariantRecalibrator \\
              -V ${localVcf.getAbsolutePath} \\
              -O ${recalOutput.getAbsolutePath} \\
              --tranches-file ${trancheOutput.getAbsolutePath} \\
              --trust-all-polymorphic \\
              $tranches \\
              $annotations \\
              -mode SNP \\
              ${GATK.skipGcs} \\
              --max-gaussians 6 \\
              -resource hapmap,known=false,training=true,truth=true,prior=15:${hapmap.getAbsolutePath} \\
              -resource omni,known=false,training=true,truth=true,prior=12:${omni.getAbsolutePath} \\
              -resource 1000G,known=false,training=true,truth=false,prior=10:${oneKg.getAbsolutePath} \\
              -resource dbsnp,known=true,training=false,truth=false,prior=7:${knownSites}
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = input.vcf.vcf.name + ".vqsr.train.snp"

              val expectedRecalIndex =
                new File(recalOutput.getAbsolutePath + ".idx")

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                recal <- SharedFile(recalOutput, nameStub + ".recal", true)
                recalIdx <- SharedFile(expectedRecalIndex,
                                       nameStub + ".recal.idx",
                                       true)
                tranches <- SharedFile(trancheOutput,
                                       nameStub + ".tranches",
                                       true)

              } yield
                TrainVQSRResult(recal = recal,
                                recalIdx = recalIdx,
                                tranches = tranches)
            }

          } yield result
    }
  val trainIndelVQSR =
    AsyncTask[TrainIndelVQSRInput, TrainVQSRResult]("__train-vqsr-indel", 1) {
      case input =>
        implicit computationEnvironment =>
          for {
            localVcf <- input.vcf.localFile
            millsAnd1Kg <- input.millsAnd1Kg.localFile
            knownSites <- input.knownSites.localFile
            result <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val recalOutput = Files.createTempFile(".recal")
              val trancheOutput = Files.createTempFile(".tranches")
              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              /* values are from:
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               */
              val tranches = List(100.0, 99.95, 99.9, 99.5, 99.0, 97.0, 96.0,
                95.0, 94.0, 93.5, 93.0, 92.0, 91.0,
                90.0).mkString(" -tranche ", " -tranche ", "")

              /* values are from:
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               */
              val annotations =
                List("FS", "ReadPosRankSum", "MQRankSum", "QD", "SOR", "DP")
                  .mkString(" -an ", " -an ", "")

              Exec.bash(logDiscriminator = "vqsr-train-indel",
                        onError = Exec.ThrowIfNonZero)(s"""\\
        java ${JVM.g1} $javaTmpDir ${JVM.maxHeap} \\
          ${GATK.javaArguments(5)} \\
            -jar $gatkJar VariantRecalibrator \\
              -V ${localVcf.getAbsolutePath} \\
              -O ${recalOutput.getAbsolutePath} \\
              --tranches-file ${trancheOutput.getAbsolutePath} \\
              --trust-all-polymorphic \\
              $tranches \\
              $annotations \\
              -mode INDEL \\
              ${GATK.skipGcs} \\
              --max-gaussians 4 \\
              -resource mills,known=false,training=true,truth=true,prior=12:${millsAnd1Kg} \\
              -resource dbsnp,known=true,training=false,truth=false,prior=2:${knownSites} \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = input.vcf.vcf.name + ".vqsr.train.indel"

              val expectedRecalIndex =
                new File(recalOutput.getAbsolutePath + ".idx")

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                recal <- SharedFile(recalOutput, nameStub + ".recal", true)
                recalIdx <- SharedFile(expectedRecalIndex,
                                       nameStub + ".recal.idx",
                                       true)
                tranches <- SharedFile(trancheOutput,
                                       nameStub + ".tranches",
                                       true)

              } yield
                TrainVQSRResult(recal = recal,
                                recalIdx = recalIdx,
                                tranches = tranches)
            }

          } yield result
    }

  def createIntervals(dict: File): Seq[(String, (Int, Int))] = {
    val samSequenceDictionary = Fasta.parseDict(dict)
    samSequenceDictionary.getSequences.asScala.toList.flatMap { sequence =>
      val contigName = sequence.getSequenceName
      val contigSize = sequence.getSequenceLength
      val intervals =
        GATK.create1BasedClosedIntervals(minInclusive = 1,
                                         maxInclusive = contigSize,
                                         numberOfIntervals = 10)
      intervals.map(i => (contigName, i))

    }
  }

  val genotypeGvcfs =
    AsyncTask[GenotypeGVCFsInput, GenotypeGVCFsResult]("__genotypegvcfs", 1) {
      case input =>
        implicit computationEnvironment =>
          releaseResources

          def intoVQSRIntermediateFolder[T] =
            appendToFilePrefix[T](Seq("vqsr_intermediate", input.name))
          def intoScattersFolder[T] =
            appendToFilePrefix[T](Seq("genotypegvcfs_scatters", input.name))
          def intoSitesOnlyFolder[T] =
            appendToFilePrefix[T](Seq("sitesOnly_intermediate", input.name))

          def vqsr(gatheredSites: VCF,
                   scatteredGenotypes: Seq[VCF],
                   training: VQSRTrainingFiles) =
            for {
              vqsrIndelModel <- intoVQSRIntermediateFolder {
                implicit computationEnvironment =>
                  trainIndelVQSR(
                    TrainIndelVQSRInput(gatheredSites,
                                        millsAnd1Kg = training.millsAnd1Kg,
                                        knownSites = training.dbSnp138))(
                    ResourceConfig.vqsrTrainIndel)
              }
              vqsrSnpModel <- intoVQSRIntermediateFolder {
                implicit computationEnvironment =>
                  trainSnpVQSR(
                    TrainSnpVQSRInput(gatheredSites,
                                      hapmap = training.hapmap,
                                      omni = training.oneKgOmni,
                                      oneKg = training.oneKgHighConfidenceSnps,
                                      knownSites = training.dbSnp138))(
                    ResourceConfig.vqsrTrainSnp)
              }
              scatteredRecalibrated <- traverseAll(scatteredGenotypes) { vcf =>
                intoScattersFolder { implicit computationEnvironment =>
                  applyVQSR(
                    ApplyVQSRInput(vcf,
                                   snp = vqsrSnpModel,
                                   indel = vqsrIndelModel))(
                    ResourceConfig.vqsrApply)
                }
              }
              recalibratedSitesOnly <- applyVQSR(
                ApplyVQSRInput(gatheredSites,
                               snp = vqsrSnpModel,
                               indel = vqsrIndelModel))(
                ResourceConfig.vqsrApply)
              _ <- Future.traverse(scatteredGenotypes)(_.vcf.delete)
              _ <- gatheredSites.vcf.delete
            } yield (recalibratedSitesOnly, scatteredRecalibrated)

          for {
            dict <- input.reference.dict
            contigs <- input.contigsFile
              .map(_.readContigs)
              .getOrElse(Future.successful(HaplotypeCaller.defaultContigs))
            intervals = HaplotypeCaller
              .createIntervals(dict)
              .filter { case (contig, _) => contigs.contains(contig) }
              .map { case (contig, (from, to)) => s"$contig:$from-$to" }
            scattered <- traverseAll(intervals) { interval =>
              intoScattersFolder { implicit computationEnvironment =>
                val scratchNeeded =
                  (input.targetVcfs.size * ResourceConfig.genotypeGvcfScratchSpaceMegabytePerSample).toInt

                val genotypeGvcfResourceRequest =
                  ResourceConfig.genotypeGvcfs.copy(
                    cpuMemoryRequest =
                      ResourceConfig.genotypeGvcfs.cpuMemoryRequest
                        .copy(scratch = scratchNeeded))

                genotypeGvcfsOnInterval(
                  GenotypeGVCFsOnIntervalInput(input.targetVcfs,
                                               input.reference,
                                               input.dbSnpVcf,
                                               interval,
                                               input.name,
                                               input.atSitesOnly))(
                  genotypeGvcfResourceRequest)
              }
            }
            scatteredGenotypes = scattered.map(_.genotypes)
            scatteredSites = scattered.map(_.sites)
            gatheredSites <- intoSitesOnlyFolder {
              implicit computationEnvironment =>
                mergeVcfs(
                  MergeVCFInput(scatteredSites,
                                input.name + ".sites_only.vcf.gz"))(
                  ResourceConfig.minimal)
            }

            (recalibratedSitesOnly, recalibratedScatteredGenotypes) <- {
              input.vqsrTrainingFiles match {
                case None =>
                  Future.successful((gatheredSites, scatteredGenotypes))
                case Some(vqsrTrainingFiles) =>
                  vqsr(gatheredSites, scatteredGenotypes, vqsrTrainingFiles)
              }
            }

            _ <- Future.traverse(scatteredSites)(_.vcf.delete)

          } yield
            GenotypeGVCFsResult(recalibratedSitesOnly,
                                recalibratedScatteredGenotypes)
    }

  val genotypeGvcfsOnInterval =
    AsyncTask[GenotypeGVCFsOnIntervalInput, GenotypesOnInterval](
      "__genotypegvcf-interval",
      1) {
      case GenotypeGVCFsOnIntervalInput(vcfs,
                                        reference,
                                        dbsnp,
                                        interval,
                                        name,
                                        atSitesOnly) =>
        implicit computationEnvironment =>
          for {
            localVcfs <- Future.traverse(vcfs.toSeq)(_.localFile)
            reference <- reference.localFile
            dbsnp <- dbsnp.localFile
            atSitesOnly <- atSitesOnly match {
              case None        => Future.successful(Nil)
              case Some(sites) => Future.traverse(sites.toSeq)(_.vcf.file)
            }
            vcf <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val vcfOutput = Files.createTempFile(".vcf.gz")
              val vcfOutputFiltered =
                Files.createTempFile(".vcf.gz").getAbsolutePath
              val vcfOutputFilteredSitesOnly =
                Files.createTempFile(".vcf.gz").getAbsolutePath

              val genomeDbWorkfolder = Files.createTempFile(".gdb")
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
            ${GATK.skipGcs} \\
            $input \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)
              }

              val sitesOnlyInterval =
                if (atSitesOnly.isEmpty) ""
                else atSitesOnly.mkString(" --intervals ", " --intervals ", "")

              Exec.bashAudit(logDiscriminator = "genotypegvcfs",
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
            ${GATK.skipGcs} \\
            --intervals $interval \\
            $sitesOnlyInterval \\
            --interval-set-rule UNION \\
            -V gendb://${genomeDbWorkfolder.getAbsolutePath} \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              /* For the 54.69 magic number refer to
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/5585cdf7877104f2c61b2720ddfe7235f2fad577/JointGenotypingWf.wdl#L58
               */
              Exec.bashAudit(logDiscriminator = "variantfilter",
                             onError = Exec.ThrowIfNonZero)(s"""\\
        java -Xmx5G ${JVM.serial} \\
          ${GATK.javaArguments(compressionLevel = 5)} \\
          -jar $gatkJar VariantFiltration \\
            --filter-expression "ExcessHet > 54.69" \\
            --filter-name ExcessHet \\
            --output $vcfOutputFiltered \\
            ${GATK.skipGcs} \\
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

              val vcfOutputFilteredIndex = new File(vcfOutputFiltered + ".tbi")
              vcfOutputFilteredIndex.deleteOnExit

              val vcfOutputFilteredSitesOnlyIndex =
                new File(vcfOutputFilteredSitesOnly + ".tbi")
              vcfOutputFilteredSitesOnlyIndex.deleteOnExit

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                vcf <- SharedFile(new File(vcfOutputFiltered), nameStub, true)
                vcfIndex <- SharedFile(vcfOutputFilteredIndex,
                                       nameStub + ".tbi",
                                       true)
                sitesOnlyVcf <- SharedFile(
                  new File(vcfOutputFilteredSitesOnly),
                  nameStub.stripSuffix("vcf.gz") + "sites_only.vcf.gz",
                  true)
                sitesOnlyVcfIdx <- SharedFile(
                  vcfOutputFilteredSitesOnlyIndex,
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
      case HaplotypeCallerInput(bam, reference, contigsFile) =>
        implicit computationEnvironment =>
          releaseResources

          def intoScattersFolder[T] =
            appendToFilePrefix[T](
              Seq("haplotypecaller_scatters", bam.bam.name.stripSuffix(".bam")))

          for {
            dict <- reference.dict
            contigs <- contigsFile
              .map(_.readContigs)
              .getOrElse(Future.successful(HaplotypeCaller.defaultContigs))
            intervals = BaseQualityScoreRecalibration
              .createIntervals(dict)
              .filter(c => contigs.contains(c))
            scattered <- traverseAll(intervals) { interval =>
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

            val tmpStdOut = Files.createTempFile(".stdout")
            val tmpStdErr = Files.createTempFile(".stderr")
            val vcfOutput = Files.createTempFile(".vcf.gz").getAbsolutePath

            val javaTmpDir =
              s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

            val input = " --INPUT " + localVcfs
              .map(_.getAbsolutePath)
              .mkString(" --INPUT ")

            Exec.bashAudit(logDiscriminator = "gathervcf",
                           onError = Exec.ThrowIfNonZero)(s""" \\
                  java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar MergeVcfs \\
                  $input \\
                  --OUTPUT $vcfOutput \\
                        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                        """)

            val expectedIndex = new File(vcfOutput + ".tbi")
            expectedIndex.deleteOnExit

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

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val vcfOutput = Files.createTempFile(".vcf.gz").getAbsolutePath

              val maxHeap = s"-Xmx${(resourceAllocated.memory * 0.8).toInt}m"

              val bashScript = s"""\\
         java $maxHeap ${JVM.g1} ${GATK.javaArguments(compressionLevel = 5)} \\
      -jar $gatkJar HaplotypeCaller \\
      --reference ${localReference.getAbsolutePath} \\
      --output $vcfOutput \\
      --input ${localBam.getAbsolutePath} \\
      --intervals $interval \\
      --emit-ref-confidence GVCF \\
      --max-alternate-alleles 3 \\
      ${GATK.skipGcs} \\
      --read-filter OverclippedReadFilter \\
      > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """

              Exec.bashAudit(logDiscriminator = "haplotypecaller",
                             onError = Exec.ThrowIfNonZero)(bashScript)

              val nameStub = bam.bam.name + "." + interval + ".hc.gvcf.vcf.gz"

              val vcfOutputIndex = new File(vcfOutput + ".tbi")
              vcfOutputIndex.deleteOnExit

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                vcf <- SharedFile(new File(vcfOutput), nameStub, true)
                vcfIndex <- SharedFile(vcfOutputIndex, nameStub + ".tbi", true)

              } yield VCF(vcf, Some(vcfIndex))
            }

          } yield vcf
    }

  val collectVariantCallingMetrics =
    AsyncTask[CollectVariantCallingMetricsInput, VariantCallingMetricsResult](
      "__collectvariantcallingmetrics",
      2) {
      case CollectVariantCallingMetricsInput(
          reference,
          targetVcf,
          dbSnpVcf,
          maybeEvaluationIntervals
          ) =>
        implicit computationEnvironment =>
          for {
            refDict <- reference.dict
            localTargetVcf <- targetVcf.localFile
            dbSnpVcf <- dbSnpVcf.localFile
            evaluationIntervals <- maybeEvaluationIntervals match {
              case None => Future.successful(None)
              case Some(evaluationIntervals) =>
                evaluationIntervals.file.file.map(Some(_))
            }
            result <- {

              val tmpStdOut = Files.createTempFile(".stdout")
              val tmpStdErr = Files.createTempFile(".stderr")
              val tmpOut = Files.createTempFile(".metrics").getAbsolutePath

              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              val picardStyleInterval = evaluationIntervals.map {
                evaluationIntervals =>
                  val picardStyleInterval = Files.createTempFile("")
                  Exec.bash(logDiscriminator = "collectvariantcallqc",
                            onError = Exec.ThrowIfNonZero)(s""" \\
        java ${JVM.serial} -Xmx2G -Dpicard.useLegacyParser=false -jar $picardJar BedToIntervalList \\
          --INPUT ${evaluationIntervals.getAbsolutePath} \\
          --SEQUENCE_DICTIONARY ${refDict.getAbsolutePath} \\
          --OUTPUT ${picardStyleInterval.getAbsolutePath} \\
        """)
                  picardStyleInterval
              }

              val restrictToInteval = picardStyleInterval
                .map { picardStyleInterval =>
                  s"--TARGET_INTERVALS ${picardStyleInterval.getAbsolutePath}"
                }
                .getOrElse("")

              Exec.bash(logDiscriminator = "collectvariantcallqc",
                        onError = Exec.ThrowIfNonZero)(s""" \\
                   java ${JVM.serial} -Xmx3G $javaTmpDir -Dpicard.useLegacyParser=false -jar $picardJar CollectVariantCallingMetrics \\
                    --INPUT ${localTargetVcf.getAbsolutePath} \\
                    --OUTPUT $tmpOut \\
                    --DBSNP ${dbSnpVcf.getAbsolutePath} \\
                    --SEQUENCE_DICTIONARY ${refDict.getAbsolutePath} \\
                    $restrictToInteval \\
                    --GVCF_INPUT true \\
                   > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
                        """)

              picardStyleInterval.foreach(_.delete)

              val expectedDetails =
                new File(tmpOut + ".variant_calling_detail_metrics")
              val expectedSummary =
                new File(tmpOut + ".variant_calling_summary_metrics")
              val nameStub = targetVcf.vcf.name + ".qc" + picardStyleInterval
                .map(_ => ".interval")
                .getOrElse("")
              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                detail <- SharedFile(
                  expectedDetails,
                  nameStub + ".variant_calling_detail_metrics",
                  true)
                summary <- SharedFile(
                  expectedSummary,
                  nameStub + ".variant_calling_summary_metrics",
                  true)

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

object TrainVQSRResult {
  implicit val encoder: Encoder[TrainVQSRResult] =
    deriveEncoder[TrainVQSRResult]
  implicit val decoder: Decoder[TrainVQSRResult] =
    deriveDecoder[TrainVQSRResult]
}

object TrainIndelVQSRInput {
  implicit val encoder: Encoder[TrainIndelVQSRInput] =
    deriveEncoder[TrainIndelVQSRInput]
  implicit val decoder: Decoder[TrainIndelVQSRInput] =
    deriveDecoder[TrainIndelVQSRInput]
}

object TrainSnpVQSRInput {
  implicit val encoder: Encoder[TrainSnpVQSRInput] =
    deriveEncoder[TrainSnpVQSRInput]
  implicit val decoder: Decoder[TrainSnpVQSRInput] =
    deriveDecoder[TrainSnpVQSRInput]
}
object ApplyVQSRInput {
  implicit val encoder: Encoder[ApplyVQSRInput] =
    deriveEncoder[ApplyVQSRInput]
  implicit val decoder: Decoder[ApplyVQSRInput] =
    deriveDecoder[ApplyVQSRInput]
}
object JointCallInput {
  implicit val encoder: Encoder[JointCallInput] =
    deriveEncoder[JointCallInput]
  implicit val decoder: Decoder[JointCallInput] =
    deriveDecoder[JointCallInput]
}
object SingleSampleVariantDiscoveryInput {
  implicit val encoder: Encoder[SingleSampleVariantDiscoveryInput] =
    deriveEncoder[SingleSampleVariantDiscoveryInput]
  implicit val decoder: Decoder[SingleSampleVariantDiscoveryInput] =
    deriveDecoder[SingleSampleVariantDiscoveryInput]
}
object SingleSampleVariantDiscoveryResult {
  implicit val encoder: Encoder[SingleSampleVariantDiscoveryResult] =
    deriveEncoder[SingleSampleVariantDiscoveryResult]
  implicit val decoder: Decoder[SingleSampleVariantDiscoveryResult] =
    deriveDecoder[SingleSampleVariantDiscoveryResult]
}
object MergeSingleCallsInput {
  implicit val encoder: Encoder[MergeSingleCallsInput] =
    deriveEncoder[MergeSingleCallsInput]
  implicit val decoder: Decoder[MergeSingleCallsInput] =
    deriveDecoder[MergeSingleCallsInput]
}
object CombineGenotypesInput {
  implicit val encoder: Encoder[CombineGenotypesInput] =
    deriveEncoder[CombineGenotypesInput]
  implicit val decoder: Decoder[CombineGenotypesInput] =
    deriveDecoder[CombineGenotypesInput]
}
