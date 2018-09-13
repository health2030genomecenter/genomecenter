package org.gc.pipelines

import org.gc.pipelines.application._

object PipelineConfiguration {

  val eventSource =
    CompositeSequencingCompleteEventSource(
      FolderWatcherEventSource("/data/UHTS/raw/instrument1/",
                               "SequencingComplete.txt",
                               "samplesheet.txt"),
      FolderWatcherEventSource("/data/UHTS/raw/instrument2/",
                               "SequencingComplete.txt",
                               "samplesheet.txt")
    )

  val pipelineState = new InMemoryPipelineState

}
