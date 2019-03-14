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

  /* GATK flag to skip Google Cloud connection
   *
   * GATK tries to connect to GCS on each run
   * A private fork of GATK introduced a flag --skip-gcs-http-calls which suppresses these tries
   *
   * GATK fails on unknown CLI flags, thus our flag should only be used with our fork of GATK
   *
   * We use the forked GATK solely in tests
   */
  def skipGcs =
    if (VersionConfig.gatkResourceName.contains("gc871d86"))
      s" --skip-gcs-http-calls "
    else ""
}
