package org.gc.pipelines.stages

import org.scalatest._

class DemultiplexingTestSuite
    extends FunSuite
    with Matchers
    with GivenWhenThen
    with TestHelpers {
  test("parse read length from use bases mask") {

    DemultiplexingInput.parseReadLengthFromBcl2FastqArguments(
      Seq(
        "--use-bases-mask",
        "y26,i8,y98,yy26yyn8y3,y75n",
        "--minimum-trimmed-read-length=8",
        "--mask-short-adapter-reads=8",
        "--ignore-missing-positions",
        "--ignore-missing-controls",
        "--ignore-missing-filter",
        "--ignore-missing-bcls"
      )) shouldBe Map(1 -> 26, 2 -> 98, 3 -> 32, 4 -> 75)
  }

}
