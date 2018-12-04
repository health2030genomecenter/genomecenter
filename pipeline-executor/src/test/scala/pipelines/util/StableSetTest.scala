package org.gc.pipelines.util

import org.scalatest._

class StableSetTestSuite extends FunSuite with Matchers {
  test("maintain stable sort order irrespective of constructor") {
    StableSet(1).toSeq shouldBe Seq(1)
    StableSet(2, 1).toSeq shouldBe Seq(1, 2)
    StableSet(1, 2).toSeq shouldBe Seq(1, 2)
    StableSet(1, 2, 3).toSeq shouldBe Seq(1, 2, 3)
    StableSet(3, 2, 1).toSeq shouldBe Seq(1, 2, 3)
    StableSet(1, 3, 2).toSeq shouldBe Seq(1, 2, 3)
    StableSet(2, 3, 1).toSeq shouldBe Seq(1, 2, 3)
    StableSet(2, 1, 3).toSeq shouldBe Seq(1, 2, 3)

  }
}
