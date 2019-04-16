package org.gc.pipelines

import tasks._
import com.typesafe.config.ConfigFactory
import org.gc.pipelines.stages._
import org.gc.pipelines.util._
import org.gc.pipelines.util.Config.option
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Future

object VariantCallingEntrypoint extends App {

  val config = ConfigFactory.load

  def doCalls(bam: CoordinateSortedBam,
              referenceFasta: ReferenceFasta,
              contigsFile: Option[ContigsFile],
              dbSnpVcf: VCF,
              vqsrTrainingFiles: Option[VQSRTrainingFiles])(
      implicit tsc: TaskSystemComponents) =
    for {
      indexedReference <- BWAAlignment.indexReference(referenceFasta)(
        ResourceConfig.indexReference
      )

      haplotypeCallerReferenceCalls <- HaplotypeCaller.haplotypeCaller(
        HaplotypeCallerInput(bam, indexedReference, contigsFile))(
        ResourceConfig.minimal)

      GenotypeGVCFsResult(_, genotypesScattered) <- HaplotypeCaller
        .genotypeGvcfs(
          GenotypeGVCFsInput(
            StableSet(haplotypeCallerReferenceCalls),
            indexedReference,
            dbSnpVcf,
            bam.bam.name + ".single",
            vqsrTrainingFiles = vqsrTrainingFiles,
            contigsFile = contigsFile
          ))(ResourceConfig.minimal)

      genotypedVcf <- HaplotypeCaller.mergeVcfs(
        MergeVCFInput(genotypesScattered,
                      bam.bam.name + ".single.genotyped.vcf.gz"))(
        ResourceConfig.minimal
      )
    } yield genotypedVcf

  withTaskSystem { implicit tsc =>
    import tasks.util.Uri
    val bams =
      Future.traverse(config.getStringList("bams").asScala.grouped(2)) {
        paths =>
          for {
            bam <- SharedFile(Uri("file://" + paths(0)))
            bai <- SharedFile(Uri("file://" + paths(1)))
          } yield CoordinateSortedBam(bam, bai)

      }

    val vqsrTrainingFiles = {
      val cvqsrMillsAnd1Kg =
        option(config, "vqsrMillsAnd1Kg")(c => p => c.getString(p))
      val cvqsrHapmap = option(config, "vqsrHapmap")(c => p => c.getString(p))
      val cvqsrOneKgOmni =
        option(config, "vqsrOneKgOmni")(c => p => c.getString(p))
      val cvqsrOneKgHighConfidenceSnps =
        option(config, "vqsrOneKgHighConfidenceSnps")(c => p => c.getString(p))
      val cvqsrDbSnp138 =
        option(config, "vqsrDbSnp138")(c => p => c.getString(p))

      if (cvqsrMillsAnd1Kg.isDefined) {
        for {
          vqsrMillsAnd1Kg <- SharedFile(Uri("file://" + cvqsrMillsAnd1Kg.get))
          vqsrMillsAnd1KgI <- SharedFile(
            Uri("file://" + cvqsrMillsAnd1Kg.get + ".tbi"))
          vqsrHapmap <- SharedFile(Uri("file://" + cvqsrHapmap.get))
          vqsrHapmapI <- SharedFile(Uri("file://" + cvqsrHapmap.get + ".tbi"))
          vqsrOneKgOmni <- SharedFile(Uri("file://" + cvqsrOneKgOmni.get))
          vqsrOneKgOmniI <- SharedFile(
            Uri("file://" + cvqsrOneKgOmni.get + ".tbi"))
          vqsrOneKgHighConfidenceSnps <- SharedFile(
            Uri("file://" + cvqsrOneKgHighConfidenceSnps.get))
          vqsrOneKgHighConfidenceSnpsI <- SharedFile(
            Uri("file://" + cvqsrOneKgHighConfidenceSnps.get + ".tbi"))
          vqsrDbSnp138 <- SharedFile(Uri("file://" + cvqsrDbSnp138.get))
          vqsrDbSnp138I <- SharedFile(
            Uri("file://" + cvqsrDbSnp138.get + ".tbi"))
        } yield
          Some(
            VQSRTrainingFiles(
              millsAnd1Kg = VCF(vqsrMillsAnd1Kg, Some(vqsrMillsAnd1KgI)),
              oneKgHighConfidenceSnps = VCF(vqsrOneKgHighConfidenceSnps,
                                            Some(vqsrOneKgHighConfidenceSnpsI)),
              hapmap = VCF(vqsrHapmap, Some(vqsrHapmapI)),
              oneKgOmni = VCF(vqsrOneKgOmni, Some(vqsrOneKgOmniI)),
              dbSnp138 = VCF(vqsrDbSnp138, Some(vqsrDbSnp138I))
            ))
      } else Future.successful(None)
    }

    val vcfs = for {
      bams <- bams
      reference <- SharedFile(Uri("file://" + config.getString("reference")))
        .map(ReferenceFasta(_))
      dbSnpVcf <- SharedFile(Uri("file://" + config.getString("dbsnpvcf")))
        .map(VCF(_, None))
      vqsrTrainingFiles <- vqsrTrainingFiles

      _ <- Future.traverse(bams) { bam =>
        doCalls(bam, reference, None, dbSnpVcf, vqsrTrainingFiles)
      }
    } yield ()

    import scala.concurrent.Await
    import scala.concurrent.duration._
    Await.result(vcfs, atMost = 100000 hours)
  }

}
