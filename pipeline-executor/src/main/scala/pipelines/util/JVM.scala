package org.gc.pipelines.util

object JVM {

  /** Java garbage collector configuraiton
    *
    * The default heuristics of the jvm are not suitable for our use case which put many small-ish jvms in the same machine.
    *
    * To enable G1: -XX:+UseG1GC -XX:ParallelGCThreads=2 -XX:ConcGCThreads=2
    * To enable serial: -XX:+UseSerialGC
    * To keep the default: empty string
    *
    * Batch application cares for the throughput and not the gc-pauses
    * furthermore threads should be limited to prevent interference with other processes.
    *
    */
  val g1 = " -XX:+UseG1GC -XX:ParallelGCThreads=2 -XX:ConcGCThreads=2 "
  val serial = " -XX:+UseSerialGC "

}
