package org.gc.pipelines

import shapeless.tag.@@
import shapeless.tag
import org.gc.pipelines.util.Circe.stringCodec

package object model {

  trait LaneTag
  type Lane = String @@ LaneTag
  def Lane(s: String): Lane = tag[LaneTag][String](s)
  implicit val (laneEncoder, laneDecoder) = stringCodec[LaneTag]

  trait ProjectTag
  type Project = String @@ ProjectTag
  def Project(s: String): Project = tag[ProjectTag][String](s)
  implicit val (projectEncoder, projectDecoder) = stringCodec[ProjectTag]

  trait SampleIdTag
  type SampleId = String @@ SampleIdTag
  def SampleId(s: String): SampleId = tag[SampleIdTag][String](s)
  implicit val (sampleIdEncoder, sampleIdDecoder) = stringCodec[SampleIdTag]

  trait ReadTypeTag
  type ReadType = String @@ ReadTypeTag
  def ReadType(s: String): ReadType = tag[ReadTypeTag][String](s)
  implicit val (readTypeEncoder, readTypeDecoder) = stringCodec[ReadTypeTag]

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

}
