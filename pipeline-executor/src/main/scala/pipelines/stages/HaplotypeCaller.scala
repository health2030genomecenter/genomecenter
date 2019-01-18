package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe.{Encoder, Decoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import scala.concurrent.Future
import fileutils.TempFile
import org.gc.pipelines.util.{Exec, GATK, JVM, ResourceConfig, Files, StableSet}
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

case class GenotypeGVCFsOnIntervalInput(targetVcfs: StableSet[VCF],
                                        reference: IndexedReferenceFasta,
                                        dbSnpVcf: VCF,
                                        interval: String,
                                        name: String)
    extends WithSharedFiles(
      targetVcfs.toSeq
        .flatMap(_.files) ++ reference.files ++ dbSnpVcf.files: _*)

case class GenotypeGVCFsInput(targetVcfs: StableSet[VCF],
                              reference: IndexedReferenceFasta,
                              dbSnpVcf: VCF,
                              name: String,
                              vqsrTrainingFiles: Option[VQSRTrainingFiles])
    extends WithSharedFiles(
      targetVcfs.toSeq
        .flatMap(_.files) ++ reference.files ++ dbSnpVcf.files: _*)

case class GenotypesOnInterval(genotypes: VCF, sites: VCF)
    extends WithSharedFiles(genotypes.files ++ sites.files: _*)

case class GenotypeGVCFsResult(sites: VCF, genotypesScattered: Seq[VCF])
    extends WithSharedFiles(
      sites.files ++ genotypesScattered.flatMap(_.files): _*)

case class TrainSnpVQSRInput(
    vcf: VCF,
    hapmap: VCF,
    omni: VCF,
    oneKg: VCF,
    knownSites: VCF,
) extends WithSharedFiles(
      vcf.files ++ hapmap.files ++ omni.files ++ oneKg.files ++ knownSites.files: _*)

case class TrainIndelVQSRInput(
    vcf: VCF,
    millsAnd1Kg: VCF,
    knownSites: VCF,
) extends WithSharedFiles(
      vcf.files ++ millsAnd1Kg.files ++ knownSites.files: _*)

case class TrainVQSRResult(
    recal: SharedFile,
    tranches: SharedFile
) extends WithSharedFiles(recal, tranches)

case class ApplyVQSRInput(
    vcf: VCF,
    snpRecal: SharedFile,
    snpTranches: SharedFile,
    indelRecal: SharedFile,
    indelTranches: SharedFile
) extends WithSharedFiles(
      vcf.files ++ List(snpRecal, snpTranches, indelRecal, indelTranches): _*)

case class JointCallInput(
    haplotypeCallerReferenceCalls: StableSet[VCF],
    indexedReference: IndexedReferenceFasta,
    dbSnpVcf: VCF,
    vqsrTrainingFiles: Option[VQSRTrainingFiles],
    outputFileName: String
) extends WithSharedFiles(
      haplotypeCallerReferenceCalls.toSeq.flatMap(_.files): _*)

object HaplotypeCaller {

  val jointCall =
    AsyncTask[JointCallInput, VCF]("__joint-call", 1) {
      case JointCallInput(haplotypeCallerReferenceCalls,
                          indexedReference,
                          dbSnpVcf,
                          vqsrTrainingFiles,
                          outputFileName) =>
        implicit computationEnvironment =>
          releaseResources

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
                    vqsrTrainingFiles = vqsrTrainingFiles
                  ))(ResourceConfig.minimal)
            }

            genotypesVCF <- HaplotypeCaller.mergeVcfs(
              MergeVCFInput(genotypesScattered,
                            outputFileName + ".joint.genotyped.vcf.gz"))(
              ResourceConfig.minimal)

          } yield genotypesVCF

    }

  val applyVQSR =
    AsyncTask[ApplyVQSRInput, VCF]("__apply-vqsr", 1) {
      case ApplyVQSRInput(vcf,
                          snpRecal,
                          snpTranches,
                          indelRecal,
                          indelTranches) =>
        implicit computationEnvironment =>
          for {
            localVCF <- vcf.localFile
            snpRecal <- snpRecal.file
            snpTranches <- snpTranches.file
            indelRecal <- indelRecal.file
            indelTranches <- indelTranches.file
            result <- {

              val gatkJar = BaseQualityScoreRecalibration.extractGatkJar()
              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val tmpVcf = TempFile.createTempFile(".tmp.vcf.gz")
              val finalVcf = TempFile.createTempFile(".vcf.gz")
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
              --truth-sensitivity-filter-level 99.7
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              Exec.bash(logDiscriminator = "vqsr-apply-indel",
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
              --truth-sensitivity-filter-level 99.7
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = vcf.vcf.name.stripSuffix(".vcf.gz") + ".vqsr"
              tmpVcf.delete

              val expectVcfIndex = new File(finalVcf.getAbsolutePath + ".tbi")

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
              val gatkJar = BaseQualityScoreRecalibration.extractGatkJar()

              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val recalOutput = TempFile.createTempFile(".recal")
              val trancheOutput = TempFile.createTempFile(".tranches")
              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              /* values are from the broad worfklow */
              val tranches =
                List(100.0, 99.95, 99.9, 99.8, 99.6, 99.5, 99.4, 99.3, 99.0,
                  98.0, 97.0, 90.0).mkString("-tranche", "-tranche", "")

              /* values are from the broad worfklow
               *  I removed DP annotation as some suggest
               *  that it is not appropriate for exomes due to high variation
               *  in coverage due to capture
               */
              val annotations =
                List("QD", "MQRankSum", "ReadPosRankSum", "FS", "MQ", "SOR")
                  .mkString(" -an ", " -an ", "")

              Exec.bash(logDiscriminator = "vqsr-train-indel",
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
              --max-gaussians 6 \\
              -resource hapmap,known=false,training=true,truth=true,prior=15:${hapmap.getAbsolutePath} \\
              -resource omni,known=false,training=true,truth=true,prior=12:${omni.getAbsolutePath} \\
              -resource 1000G,known=false,training=true,truth=false,prior=10:${oneKg.getAbsolutePath} \\
              -resource dbsnp,known=true,training=false,truth=false,prior=7:${knownSites}
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = input.vcf.vcf.name + ".vqsr.train.snp"

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                recal <- SharedFile(recalOutput, nameStub + ".recal", true)
                tranches <- SharedFile(trancheOutput,
                                       nameStub + ".tranches",
                                       true)

              } yield TrainVQSRResult(recal = recal, tranches = tranches)
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
              val gatkJar = BaseQualityScoreRecalibration.extractGatkJar()

              val tmpStdOut = TempFile.createTempFile(".stdout")
              val tmpStdErr = TempFile.createTempFile(".stderr")
              val recalOutput = TempFile.createTempFile(".recal")
              val trancheOutput = TempFile.createTempFile(".tranches")
              val javaTmpDir =
                s""" -Djava.io.tmpdir=${System.getProperty("java.io.tmpdir")} """

              /* values are from the broad worfklow
               * https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/blob/904564ad46af8d69ffc4077b579185317b2dc53b/JointGenotypingWf.hg38.inputs.json
               */
              val tranches = List(100.0, 99.95, 99.9, 99.5, 99.0, 97.0, 96.0,
                95.0, 94.0, 93.5, 93.0, 92.0, 91.0,
                90.0).mkString(" -tranche ", " -tranche ", "")

              /* values are from the broad worfklow */
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
              --max-gaussians 4 \\
              -resource mills,known=false,training=true,truth=true,prior=12:${millsAnd1Kg} \\
              -resource dbsnp,known=true,training=false,truth=false,prior=2:${knownSites} \\
        > >(tee -a ${tmpStdOut.getAbsolutePath}) 2> >(tee -a ${tmpStdErr.getAbsolutePath} >&2)
      """)

              val nameStub = input.vcf.vcf.name + ".vqsr.train.indel"

              for {
                _ <- SharedFile(tmpStdOut, nameStub + ".stdout", true)
                _ <- SharedFile(tmpStdErr, nameStub + ".stderr", true)
                recal <- SharedFile(recalOutput, nameStub + ".recal", true)
                tranches <- SharedFile(trancheOutput,
                                       nameStub + ".tranches",
                                       true)

              } yield TrainVQSRResult(recal = recal, tranches = tranches)
            }

          } yield result
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
              scatteredRecalibrated <- Future.traverse(scatteredGenotypes) {
                vcf =>
                  intoScattersFolder { implicit computationEnvironment =>
                    applyVQSR(
                      ApplyVQSRInput(vcf,
                                     snpRecal = vqsrSnpModel.recal,
                                     snpTranches = vqsrSnpModel.tranches,
                                     indelRecal = vqsrIndelModel.recal,
                                     indelTranches = vqsrIndelModel.tranches))(
                      ResourceConfig.vqsrApply)
                  }
              }
              recalibratedSitesOnly <- applyVQSR(
                ApplyVQSRInput(gatheredSites,
                               snpRecal = vqsrSnpModel.recal,
                               snpTranches = vqsrSnpModel.tranches,
                               indelRecal = vqsrIndelModel.recal,
                               indelTranches = vqsrIndelModel.tranches))(
                ResourceConfig.vqsrApply)
              _ <- Future.traverse(scatteredGenotypes)(_.vcf.delete)
              _ <- gatheredSites.vcf.delete
            } yield (recalibratedSitesOnly, scatteredRecalibrated)

          for {
            dict <- input.reference.dict
            intervals = BaseQualityScoreRecalibration
              .createIntervals(dict)
              .filterNot(_ == "unmapped")
            scattered <- Future.traverse(intervals) { interval =>
              intoScattersFolder { implicit computationEnvironment =>
                genotypeGvcfsOnInterval(
                  GenotypeGVCFsOnIntervalInput(input.targetVcfs,
                                               input.reference,
                                               input.dbSnpVcf,
                                               interval,
                                               input.name))(
                  ResourceConfig.genotypeGvcfs)
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
                                        name) =>
        implicit computationEnvironment =>
          for {
            localVcfs <- Future.traverse(vcfs.toSeq)(_.localFile)
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
            appendToFilePrefix[T](
              Seq("haplotypecaller_scatters", bam.bam.name.stripSuffix(".bam")))

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
