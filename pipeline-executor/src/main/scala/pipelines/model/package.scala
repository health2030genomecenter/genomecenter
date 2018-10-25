package org.gc.pipelines

import shapeless.tag.@@
import shapeless.tag
import org.gc.pipelines.util.Circe.{stringCodec, intCodec}

package object model {

  trait LaneTag
  type Lane = Int @@ LaneTag
  def Lane(s: Int): Lane = tag[LaneTag][Int](s)
  implicit val (laneEncoder, laneDecoder) = intCodec[LaneTag]

  trait ProjectTag
  type Project = String @@ ProjectTag
  def Project(s: String): Project = tag[ProjectTag][String](s)
  implicit val (projectEncoder, projectDecoder) = stringCodec[ProjectTag]

  trait SampleIdTag
  type SampleId = String @@ SampleIdTag
  def SampleId(s: String): SampleId = tag[SampleIdTag][String](s)
  implicit val (sampleIdEncoder, sampleIdDecoder) = stringCodec[SampleIdTag]

  trait SampleNameTag
  type SampleName = String @@ SampleNameTag
  def SampleName(s: String): SampleName = tag[SampleNameTag][String](s)
  implicit val (sampleNameEncoder, sampleNameDecoder) =
    stringCodec[SampleNameTag]

  trait ReadTypeTag
  type ReadType = Int @@ ReadTypeTag
  def ReadType(s: Int): ReadType = tag[ReadTypeTag][Int](s)
  implicit val (readTypeEncoder, readTypeDecoder) = intCodec[ReadTypeTag]

  trait IndexTag
  type Index = String @@ IndexTag
  def Index(s: String): Index = tag[IndexTag][String](s)
  implicit val (indexEncoder, indexDecoder) = stringCodec[IndexTag]

  trait IndexIdTag
  type IndexId = String @@ IndexIdTag
  def IndexId(s: String): IndexId = tag[IndexIdTag][String](s)
  implicit val (indexIdEncoder, indexIdDecoder) = stringCodec[IndexIdTag]

  trait RunIdTag
  type RunId = String @@ RunIdTag
  def RunId(s: String): RunId = tag[RunIdTag][String](s)
  implicit val (runIdEncoder, runIdDecoder) = stringCodec[RunIdTag]

  trait ProcessingIdTag
  type ProcessingId = String @@ ProcessingIdTag
  def ProcessingId(s: String): ProcessingId = tag[ProcessingIdTag][String](s)
  implicit val (processIdEncoder, processIdDecoder) =
    stringCodec[ProcessingIdTag]

}
