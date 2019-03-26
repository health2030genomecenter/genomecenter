package org.gc.pipelines.cli

import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.xfer.FileSystemFile

object Sftp {

  def uploadFiles(host: String,
                  user: String,
                  password: Array[Char],
                  files: Seq[(String, String)]) = {
    val ssh = new SSHClient();
    ssh.loadKnownHosts();
    ssh.connect(host);
    try {
      ssh.authPassword(user, password)
      val sftp = ssh.newSFTPClient()

      def mkdirs(to: String) = {
        val folders = to
          .split("/")
          .scanLeft(List.empty[String])((acc, elem) => acc :+ elem)
          .drop(1)
        folders.foreach { folder =>
          scala.util.Try(sftp.mkdir(folder.mkString("/")))
        }
      }

      try {
        files.foreach {
          case (from, to) =>
            val existence = Option(sftp.statExistence(to))
            val sizeOnRemote = existence.map(_.getSize)
            val sizeOnLocal = new java.io.File(from).length
            if (sizeOnRemote.isEmpty || sizeOnLocal != sizeOnRemote.get) {
              println(
                s"Copy local $from to remote $to ($user@$host) $sizeOnRemote $sizeOnLocal.")
              mkdirs(to)
              sftp.put(new FileSystemFile(from), to)
              println("Done.")
            } else {
              println(
                s"Skip copying $from to $to because it is already there. File sizes match.")
            }
        }
        println(s"${files.size} uploaded or skipped.")

      } finally {
        sftp.close();
      }
    } finally {
      ssh.disconnect();
    }
  }

}
