package org.gc.pipelines.util

object GATK {
  def javaArguments(compressionLevel: Int) =
    s" -Dsamjdk.use_async_io_read_samtools=false -Dsamjdk.use_async_io_write_samtools=true -Dsamjdk.use_async_io_write_tribble=false -Dsamjdk.compression_level=$compressionLevel "
}
