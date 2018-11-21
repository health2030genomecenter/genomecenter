package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._
import org.gc.pipelines.model._
import scala.concurrent.{ExecutionContext, Future}

case class SampleSheetFile(file: SharedFile) extends WithSharedFiles {
  def parse(implicit tsc: TaskSystemComponents, ec: ExecutionContext) = {
    implicit val mat = tsc.actorMaterializer
    file.source
      .runFold(akka.util.ByteString.empty)(_ ++ _)
      .map(_.utf8String)
      .map(SampleSheet(_).parsed)
  }
}

case class ReferenceFasta(file: SharedFile) extends WithSharedFiles(file)

case class IndexedReferenceFasta(fasta: SharedFile, indexFiles: Set[SharedFile])
    extends WithSharedFiles(fasta +: indexFiles.toSeq: _*) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- Future.traverse(indexFiles)(_.file)
      fasta <- fasta.file
    } yield fasta
  def dict(implicit tsc: TaskSystemComponents) =
    indexFiles.find(_.name.endsWith(".dict")).get.file
}

case class FastQWithSampleMetadata(project: Project,
                                   sampleId: SampleId,
                                   lane: Lane,
                                   readType: ReadType,
                                   partition: PartitionId,
                                   fastq: FastQ)
    extends WithSharedFiles(fastq.file)

case class PerSamplePerRunFastQ(
    lanes: Set[FastQPerLane],
    project: Project,
    sampleId: SampleId,
    runId: RunId
) extends WithSharedFiles(
      lanes
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq: _*) {
  def withoutRunId = PerSampleFastQ(lanes, project, sampleId)
}

case class PerSampleFastQ(
    lanes: Set[FastQPerLane],
    project: Project,
    sampleId: SampleId,
) extends WithSharedFiles(
      lanes
        .flatMap(fq => List(fq.read1.file, fq.read2.file))
        .toSeq: _*) {

  def ++(that: PerSampleFastQ) = {
    assert(this.project == that.project)
    assert(this.sampleId == that.sampleId)
    PerSampleFastQ(lanes ++ that.lanes, project, sampleId)
  }
  def runIdTag =
    lanes
      .map(_.runId)
      .toSeq
      .distinct
      .map(_.toString)
      .sorted
      .mkString(".")
}

case class FastQPerLaneWithMetadata(
    lane: FastQPerLane,
    project: Project,
    sampleId: SampleId,
) extends WithSharedFiles(lane.read1.file, lane.read2.file)

case class BamWithSampleMetadataPerLane(project: Project,
                                        sampleId: SampleId,
                                        runId: RunId,
                                        lane: Lane,
                                        bam: Bam)
    extends WithSharedFiles(bam.file)

case class BamsWithSampleMetadata(project: Project,
                                  sampleId: SampleId,
                                  bams: Set[Bam])
    extends ResultWithSharedFiles(bams.map(_.file).toSeq: _*)

case class BamWithSampleMetadata(project: Project, sampleId: SampleId, bam: Bam)
    extends WithSharedFiles(bam.files: _*)

case class CoordinateSortedBamWithSampleMetadata(project: Project,
                                                 sampleId: SampleId,
                                                 runId: RunId,
                                                 bam: CoordinateSortedBam)
    extends WithSharedFiles(bam.files: _*)

case class FastQ(file: SharedFile, numberOfReads: Long)
    extends ResultWithSharedFiles(file)

case class Bam(file: SharedFile) extends ResultWithSharedFiles(file)

case class CoordinateSortedBam(bam: SharedFile, bai: SharedFile)
    extends WithSharedFiles(bam) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- bai.file
      bam <- bam.file
    } yield bam
}
case class FastQPerLane(runId: RunId,
                        lane: Lane,
                        read1: FastQ,
                        read2: FastQ,
                        umi: Option[FastQ],
                        partition: PartitionId)

case class VCF(vcf: SharedFile, index: Option[SharedFile])
    extends WithSharedFiles(vcf +: index.toList: _*) {
  def localFile(implicit tsc: TaskSystemComponents, ec: ExecutionContext) =
    for {
      _ <- Future.traverse(index.toList)(_.file)
      vcf <- vcf.file
    } yield vcf
}

case class BQSRTable(file: SharedFile) extends WithSharedFiles(file)

case class BedFile(file: SharedFile) extends WithSharedFiles(file)

case class GTFFile(file: SharedFile) extends WithSharedFiles(file)

case class PerSamplePerLanePerReadMetrics(
    seq: Seq[(Project, SampleId, RunId, Lane, ReadType, org.gc.readqc.Metrics)])

//
// Codecs from here on
//

object FastQWithSampleMetadata {
  implicit val encoder: Encoder[FastQWithSampleMetadata] =
    deriveEncoder[FastQWithSampleMetadata]
  implicit val decoder: Decoder[FastQWithSampleMetadata] =
    deriveDecoder[FastQWithSampleMetadata]
}

object PerSamplePerLanePerReadMetrics {
  implicit val encoder: Encoder[PerSamplePerLanePerReadMetrics] =
    deriveEncoder[PerSamplePerLanePerReadMetrics]
  implicit val decoder: Decoder[PerSamplePerLanePerReadMetrics] =
    deriveDecoder[PerSamplePerLanePerReadMetrics]
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

object PerSampleFastQ {
  implicit val encoder: Encoder[PerSampleFastQ] =
    deriveEncoder[PerSampleFastQ]
  implicit val decoder: Decoder[PerSampleFastQ] =
    deriveDecoder[PerSampleFastQ]
}

object FastQPerLaneWithMetadata {
  implicit val encoder: Encoder[FastQPerLaneWithMetadata] =
    deriveEncoder[FastQPerLaneWithMetadata]
  implicit val decoder: Decoder[FastQPerLaneWithMetadata] =
    deriveDecoder[FastQPerLaneWithMetadata]
}

object BedFile {
  implicit val encoder: Encoder[BedFile] =
    deriveEncoder[BedFile]
  implicit val decoder: Decoder[BedFile] =
    deriveDecoder[BedFile]
}

object SampleSheetFile {
  implicit val encoder: Encoder[SampleSheetFile] =
    deriveEncoder[SampleSheetFile]
  implicit val decoder: Decoder[SampleSheetFile] =
    deriveDecoder[SampleSheetFile]
}

object CoordinateSortedBamWithSampleMetadata {
  implicit val encoder: Encoder[CoordinateSortedBamWithSampleMetadata] =
    deriveEncoder[CoordinateSortedBamWithSampleMetadata]
  implicit val decoder: Decoder[CoordinateSortedBamWithSampleMetadata] =
    deriveDecoder[CoordinateSortedBamWithSampleMetadata]
}

object GTFFile {
  implicit val encoder: Encoder[GTFFile] =
    deriveEncoder[GTFFile]
  implicit val decoder: Decoder[GTFFile] =
    deriveDecoder[GTFFile]
}

object PerSamplePerRunFastQ {
  implicit val encoder: Encoder[PerSamplePerRunFastQ] =
    deriveEncoder[PerSamplePerRunFastQ]
  implicit val decoder: Decoder[PerSamplePerRunFastQ] =
    deriveDecoder[PerSamplePerRunFastQ]
}
