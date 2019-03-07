package org.gc.pipelines.util

object GATK {

  def create1BasedClosedIntervals(minInclusive: Int,
                                  maxInclusive: Int,
                                  numberOfIntervals: Int) = {
    val scatterSize = (maxInclusive - minInclusive + 1) / numberOfIntervals
    (minInclusive until maxInclusive by scatterSize).map { start =>
      val end = math.min(start + scatterSize - 1, maxInclusive)
      (start, end)
    }
  }

  def javaArguments(compressionLevel: Int) =
    s" -Dsamjdk.use_async_io_read_samtools=false -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=false -Dsamjdk.compression_level=$compressionLevel "
}
