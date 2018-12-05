package org.gc.pipelines.util

import org.scalatest._

import io.circe.syntax._

class StableSetTestSuite extends FunSuite with Matchers {
  test("maintain stable sort order irrespective of constructor") {
    StableSet(1).asJson.noSpaces shouldBe "[1]"
    StableSet(2, 1).asJson.noSpaces shouldBe "[1,2]"
    StableSet(1, 2).asJson.noSpaces shouldBe "[1,2]"
    StableSet(1, 2, 3).asJson.noSpaces shouldBe "[1,2,3]"
    StableSet(3, 2, 1).asJson.noSpaces shouldBe "[1,2,3]"
    StableSet(1, 3, 2).asJson.noSpaces shouldBe "[1,2,3]"
    StableSet(2, 3, 1).asJson.noSpaces shouldBe "[1,2,3]"
    StableSet(2, 1, 3).asJson.noSpaces shouldBe "[1,2,3]"

  }
}
