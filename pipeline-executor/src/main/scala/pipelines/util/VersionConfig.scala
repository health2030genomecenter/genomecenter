package org.gc.pipelines.util

import com.typesafe.config.ConfigFactory

object VersionConfig {

  val config = ConfigFactory.load.getConfig("gc.versions")

  val gatkResourceName = config.getString("gatk")
}
