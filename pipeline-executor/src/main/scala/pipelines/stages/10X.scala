package org.gc.pipelines.stages

import org.gc.pipelines.model._
import org.gc.pipelines.util.StableSet.syntax
import org.gc.pipelines.util.{FastQHelpers}

import org.gc.pipelines.util.StableSet.syntax

import tasks._
import tasks.circesupport._
import scala.concurrent.Future

object TenXStages {
  // Assumes that the index read is in the umi field of the FastQ class
  val concatenateFastQ =
    AsyncTask[PerSampleFastQ, PerSampleFastQ]("__10x-concatenate-fastq", 2) {
      case PerSampleFastQ(lanes, project, sample) =>
        implicit computationEnvironment =>
          val perLane =
            lanes.toSeq.groupBy(lane => (lane.runId, lane.lane)).toSeq

          val concatenatedLanes = Future.traverse(perLane) {
            case ((runId, lane), group) =>
              for {
                read1Files <- Future.traverse(
                  group.toSeq.sortBy(_.read1.file.name).map(_.read1.file))(
                  _.file)
                read2Files <- Future.traverse(
                  group.toSeq.sortBy(_.read2.file.name).map(_.read2.file))(
                  _.file)
                indexFiles <- Future.traverse(
                  group.toSeq.sortBy(_.umi.get.file.name).map(_.umi.get.file))(
                  _.file)
                cat1 = FastQHelpers.cat(read1Files)
                cat2 = FastQHelpers.cat(read2Files)
                catIndex = FastQHelpers.cat(indexFiles)

                cat1SF <- SharedFile(cat1,
                                     s"${sample}_S1_L00${lane}_R1_001.fastq.gz",
                                     deleteFile = true)
                cat2SF <- SharedFile(cat2,
                                     s"${sample}_S1_L00${lane}_R2_001.fastq.gz",
                                     deleteFile = true)
                catIndexSF <- SharedFile(
                  catIndex,
                  s"${sample}_S1_L00${lane}_I1_001.fastq.gz",
                  deleteFile = true)

              } yield
                FastQPerLane(
                  runId,
                  lane,
                  read1 =
                    FastQ(cat1SF,
                          group.toSeq.map(_.read1.numberOfReads).sum,
                          group.toSeq.headOption.flatMap(_.read1.readLength)),
                  read2 =
                    FastQ(cat2SF,
                          group.toSeq.map(_.read2.numberOfReads).sum,
                          group.toSeq.headOption.flatMap(_.read2.readLength)),
                  umi = Some(
                    FastQ(
                      catIndexSF,
                      group.toSeq.map(_.umi.get.numberOfReads).sum,
                      group.toSeq.headOption.flatMap(_.umi.get.readLength))),
                  partition = PartitionId(0)
                )
          }

          for {
            concatenatedLanes <- concatenatedLanes
          } yield
            PerSampleFastQ(
              concatenatedLanes.toSet.toStable,
              project,
              sample
            )

    }

}
