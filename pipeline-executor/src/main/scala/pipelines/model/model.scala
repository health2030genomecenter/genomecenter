package org.gc.pipelines.model

case class Metadata(runId: RunId,
                    lane: Lane,
                    sample: SampleId,
                    project: Project)
