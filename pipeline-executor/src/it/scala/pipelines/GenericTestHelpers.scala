package org.gc.pipelines

import scala.concurrent._
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

object GenericTestHelpers {
  val timeout = 600 seconds
  def await[T](f: Future[T]) = Await.result(f, atMost = timeout)

  def makeTestConfig = {
    val tmp = tasks.util.TempFile.createTempFile(".temp")
    tmp.delete
    val config = ConfigFactory.parseString(
      s"""tasks.fileservice.storageURI=${tmp.getAbsolutePath}
      hosts.numCPU=12
      hosts.RAM=6000
      tasks.createFilePrefixForTaskId = false
      tasks.fileservice.allowDeletion = true
      """
    )
    (config, tmp)
  }
}
