package org.gc.pipelines.application

import org.gc.pipelines.application.migrations._

object PipelineStateMigrations {

  val migrations =
    List(Migration0000, Migration0001, Migration0002, Migration0003)
}
