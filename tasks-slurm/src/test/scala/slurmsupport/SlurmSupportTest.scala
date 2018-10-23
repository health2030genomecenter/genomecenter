package org.gc.slurmsupport

import org.scalatest._

import org.scalatest.Matchers

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

import tasks.circesupport._
import com.typesafe.config.ConfigFactory
import tasks._
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

object TestSlave extends App {
  withTaskSystem { _ =>
    Thread.sleep(100000)
  }
}

object SlurmTest extends TestHelpers {

  val testTask = AsyncTask[Input, Int]("slurmtest", 1) {
    input => implicit computationEnvironment =>
      log.info("Hello from task")
      Future(1)
  }

  val testConfig2 = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=0
      tasks.elastic.engine = "org.gc.slurmsupport.SlurmSupport"
      tasks.elastic.queueCheckInterval = 3 seconds  
      tasks.addShutdownHook = false
      tasks.failuredetector.acceptable-heartbeat-pause = 10 s
      tasks.slave-main-class = "org.gc.slurmsupport.TestSlave"
      """
    )
  }

  def run = {
    withTaskSystem(testConfig2) { implicit ts =>
      import scala.concurrent.ExecutionContext.Implicits.global

      val f1 = testTask(Input(1))(ResourceRequest(1, 500))

      val f2 = f1.flatMap(_ => testTask(Input(2))(ResourceRequest(1, 500)))
      val f3 = testTask(Input(3))(ResourceRequest(1, 500))
      val future = for {
        t1 <- f1
        t2 <- f2
        t3 <- f3
      } yield t1 + t2 + t3

      import scala.concurrent.duration._
      scala.concurrent.Await.result(future, atMost = 400000 seconds)

    }
  }

}

class SlurmTestSuite extends FunSuite with Matchers {

  test("slurm allocation should spawn nodes") {
    (SlurmTest.run.get: Int) should equal(3)

  }

}

trait TestHelpers {

  def await[T](f: Future[T]) = Await.result(f, atMost = 60 seconds)

  case class Input(i: Int)
  object Input {
    implicit val enc: Encoder[Input] = deriveEncoder[Input]
    implicit val dec: Decoder[Input] = deriveDecoder[Input]
  }

  def testConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=4
      """
    )
  }

}
