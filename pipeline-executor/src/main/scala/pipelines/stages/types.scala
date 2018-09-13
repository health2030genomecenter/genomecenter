package org.gc.pipelines.stages

import tasks._
import tasks.circesupport._
import io.circe._
import io.circe.generic.semiauto._
import org.gc.pipelines.model._

case class FastQWithSampleMetadata(project: Project,
                                   sampleId: SampleId,
                                   runId: RunId,
                                   lane: Lane,
                                   readType: ReadType,
                                   fastq: FastQ)
    extends ResultWithSharedFiles(fastq.file)

case class FastQ(file: SharedFile) extends ResultWithSharedFiles(file)

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
