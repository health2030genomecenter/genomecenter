package org.gc.pipelines.application

import org.gc.pipelines.application.migrations._

/* Migrations of the persisted events of PipelineState
 *
 * All migrations have to be listed in `migrations`.
 *
 * Each migration object migrate from the persisted schema version to
 * the most recent (current) schema version. This is contrary to the
 * usual incremental migrations.
 * E.g. if the current schema version is 5 then Migration0000 migrates
 * from version 0 to version 5 and Migration0001 from 1 to 5 directly.
 * (Not 0->1,1->2,..,4->5)
 */
object PipelineStateMigrations {

  val migrations =
    List(Migration0000, Migration0001, Migration0002, Migration0003)
}
