package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._
import org.gc.pipelines.model._
import scala.concurrent.{ExecutionContext, Future}

case class ReferenceFasta(file: SharedFile) extends WithSharedFiles(file)

case class IndexedReferenceFasta(fasta: SharedFile, indexFiles: Set[SharedFile])
    extends WithSharedFiles(fasta) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- Future.traverse(indexFiles)(_.file)
      fasta <- fasta.file
    } yield fasta
}

case class FastQWithSampleMetadata(project: Project,
                                   sampleId: SampleId,
                                   runId: RunId,
                                   lane: Lane,
                                   readType: ReadType,
                                   fastq: FastQ)
    extends WithSharedFiles(fastq.file)

case class PerSampleFastQ(
    fastqs: Set[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    runId: RunId
) extends WithSharedFiles(
      fastqs
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq: _*)

case class BamWithSampleMetadataPerLane(project: Project,
                                        sampleId: SampleId,
                                        runId: RunId,
                                        lane: Lane,
                                        bam: Bam)
    extends WithSharedFiles(bam.file)

case class BamsWithSampleMetadata(project: Project,
                                  sampleId: SampleId,
                                  runId: RunId,
                                  bams: Set[Bam])
    extends ResultWithSharedFiles(bams.map(_.file).toSeq: _*)

case class BamWithSampleMetadata(project: Project,
                                 sampleId: SampleId,
                                 runId: RunId,
                                 bam: CoordinateSortedBam)
    extends WithSharedFiles(bam.files: _*)

case class FastQ(file: SharedFile) extends ResultWithSharedFiles(file)

case class Bam(file: SharedFile) extends ResultWithSharedFiles(file)

case class CoordinateSortedBam(bam: SharedFile, bai: SharedFile)
    extends WithSharedFiles(bam) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- bai.file
      bam <- bam.file
    } yield bam
}

case class FastQPerLane(lane: Lane, read1: FastQ, read2: FastQ)

case class BWAAlignedReads(bams: Set[BamWithSampleMetadata])
    extends WithSharedFiles(bams.map(_.bam.bam).toSeq: _*)

case class RecalibratedReads(bams: Set[BamWithSampleMetadata])
    extends WithSharedFiles(bams.map(_.bam.bam).toSeq: _*)

case class VCF(vcf: SharedFile, index: Option[SharedFile])
    extends WithSharedFiles(vcf +: index.toList: _*) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- Future.traverse(index.toList)(_.file)
      vcf <- vcf.file
    } yield vcf
}

case class BQSRTable(file: SharedFile) extends WithSharedFiles(file)

//
// Codecs from here on
//

object FastQWithSampleMetadata {
  implicit val encoder: Encoder[FastQWithSampleMetadata] =
    deriveEncoder[FastQWithSampleMetadata]
  implicit val decoder: Decoder[FastQWithSampleMetadata] =
    deriveDecoder[FastQWithSampleMetadata]
}

object FastQ {
  implicit val encoder: Encoder[FastQ] =
    deriveEncoder[FastQ]
  implicit val decoder: Decoder[FastQ] =
    deriveDecoder[FastQ]
}

object Bam {
  implicit val encoder: Encoder[Bam] =
    deriveEncoder[Bam]
  implicit val decoder: Decoder[Bam] =
    deriveDecoder[Bam]
}

object CoordinateSortedBam {
  implicit val encoder: Encoder[CoordinateSortedBam] =
    deriveEncoder[CoordinateSortedBam]
  implicit val decoder: Decoder[CoordinateSortedBam] =
    deriveDecoder[CoordinateSortedBam]
}

object BamWithSampleMetadataPerLane {
  implicit val encoder: Encoder[BamWithSampleMetadataPerLane] =
    deriveEncoder[BamWithSampleMetadataPerLane]
  implicit val decoder: Decoder[BamWithSampleMetadataPerLane] =
    deriveDecoder[BamWithSampleMetadataPerLane]
}

object BamsWithSampleMetadata {
  implicit val encoder: Encoder[BamsWithSampleMetadata] =
    deriveEncoder[BamsWithSampleMetadata]
  implicit val decoder: Decoder[BamsWithSampleMetadata] =
    deriveDecoder[BamsWithSampleMetadata]
}

object BamWithSampleMetadata {
  implicit val encoder: Encoder[BamWithSampleMetadata] =
    deriveEncoder[BamWithSampleMetadata]
  implicit val decoder: Decoder[BamWithSampleMetadata] =
    deriveDecoder[BamWithSampleMetadata]
}

object ReferenceFasta {
  implicit val encoder: Encoder[ReferenceFasta] =
    deriveEncoder[ReferenceFasta]
  implicit val decoder: Decoder[ReferenceFasta] =
    deriveDecoder[ReferenceFasta]
}

object FastQPerLane {
  implicit val encoder: Encoder[FastQPerLane] =
    deriveEncoder[FastQPerLane]
  implicit val decoder: Decoder[FastQPerLane] =
    deriveDecoder[FastQPerLane]
}

object BWAAlignedReads {
  implicit val encoder: Encoder[BWAAlignedReads] =
    deriveEncoder[BWAAlignedReads]
  implicit val decoder: Decoder[BWAAlignedReads] =
    deriveDecoder[BWAAlignedReads]
}

object IndexedReferenceFasta {
  implicit val encoder: Encoder[IndexedReferenceFasta] =
    deriveEncoder[IndexedReferenceFasta]
  implicit val decoder: Decoder[IndexedReferenceFasta] =
    deriveDecoder[IndexedReferenceFasta]
}

object VCF {
  implicit val encoder: Encoder[VCF] =
    deriveEncoder[VCF]
  implicit val decoder: Decoder[VCF] =
    deriveDecoder[VCF]
}

object BQSRTable {
  implicit val encoder: Encoder[BQSRTable] =
    deriveEncoder[BQSRTable]
  implicit val decoder: Decoder[BQSRTable] =
    deriveDecoder[BQSRTable]
}

object RecalibratedReads {
  implicit val encoder: Encoder[RecalibratedReads] =
    deriveEncoder[RecalibratedReads]
  implicit val decoder: Decoder[RecalibratedReads] =
    deriveDecoder[RecalibratedReads]
}

object PerSampleFastQ {
  implicit val encoder: Encoder[PerSampleFastQ] =
    deriveEncoder[PerSampleFastQ]
  implicit val decoder: Decoder[PerSampleFastQ] =
    deriveDecoder[PerSampleFastQ]
}
