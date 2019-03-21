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
      try {
        files.foreach {
          case (from, to) =>
            val sizeOnRemote = sftp.statExistence(to).getSize
            val sizeOnLocal = new java.io.File(from).length
            if (sizeOnLocal != sizeOnRemote) {
              println(s"Copy local $from to remote $to ($user@$host).")
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
