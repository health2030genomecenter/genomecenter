package org.gc.pipelines.cli

import org.scalatest._

class SftpTestSuite extends FunSuite with Matchers with GivenWhenThen {

  ignore("upload") {
    val host = "localhost"
    val user = "foo"
    val pass = "".toSeq.toArray
    val file =
      fileutils.openFileWriter(wr => wr.write("boo"))._1.getAbsolutePath
    Sftp.uploadFiles(host, user, pass, List(file -> "upload/folder1/folder2/"))

  }

}
