package org.gc.pipelines.util

import org.scalatest._
import org.scalatest.{Matchers, BeforeAndAfterAll}

class GATKTestSuite
    extends FunSuite
    with BeforeAndAfterAll
    with GivenWhenThen
    with Matchers {
  test("create1BasedClosedIntervals") {
    org.gc.pipelines.util.GATK
      .create1BasedClosedIntervals(31, 80, 11)
      .flatMap(i => i._1 to i._2 toList) shouldBe (31 to 80).toList

  }
}
