package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._
import org.gc.pipelines.model._

case class ReferenceFasta(file: SharedFile)

case class FastQWithSampleMetadata(project: Project,
                                   sampleId: SampleId,
                                   runId: RunId,
                                   lane: Lane,
                                   readType: ReadType,
                                   fastq: FastQ)
    extends ResultWithSharedFiles(fastq.file)

case class BamWithSampleMetadataPerLane(project: Project,
                                        sampleId: SampleId,
                                        runId: RunId,
                                        lane: Lane,
                                        bam: Bam)
    extends ResultWithSharedFiles(bam.file)

case class FastQ(file: SharedFile) extends ResultWithSharedFiles(file)

case class Bam(file: SharedFile) extends ResultWithSharedFiles(file)

case class Bai(file: SharedFile) extends ResultWithSharedFiles(file)

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

object Bai {
  implicit val encoder: Encoder[Bai] =
    deriveEncoder[Bai]
  implicit val decoder: Decoder[Bai] =
    deriveDecoder[Bai]
}

object BamWithSampleMetadataPerLane {
  implicit val encoder: Encoder[BamWithSampleMetadataPerLane] =
    deriveEncoder[BamWithSampleMetadataPerLane]
  implicit val decoder: Decoder[BamWithSampleMetadataPerLane] =
    deriveDecoder[BamWithSampleMetadataPerLane]
}

object ReferenceFasta {
  implicit val encoder: Encoder[ReferenceFasta] =
    deriveEncoder[ReferenceFasta]
  implicit val decoder: Decoder[ReferenceFasta] =
    deriveDecoder[ReferenceFasta]
}
