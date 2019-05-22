package org.gc.pipelines.stages

import org.gc.pipelines.util
import org.gc.pipelines.util.VersionConfig

object Executables {

  override def toString =
    Map(
      "gatkJar" -> gatkJar,
      "fakeRscript" -> fakeRscript,
      "picarJar" -> picardJar,
      "umiprocessorJar" -> umiProcessorJar,
      "bcftools" -> bcftoolsExecutable,
      "bwa" -> bwaExecutable,
      "samtools" -> samtoolsExecutable,
      "fastp" -> fastpExecutable,
      "readQCJar" -> readQCJar,
      "star261c" -> star261cExecutable,
      "star260a" -> star261aExecutable,
      "qtlTools" -> qtlToolsExecutable
    ).toList
      .sortBy(_._1)
      .map(v => v._1 + ":" + v._2)
      .mkString("Executables(", ", ", ")")

  val gatkJar: String = {
    val gatkJarName = VersionConfig.gatkResourceName
    fileutils.TempFile
      .getExecutableFromJar(s"/bin/$gatkJarName", gatkJarName)
      .getAbsolutePath
  }

  val fakeRscript =
    fileutils.TempFile
      .getExecutableFromJar("/bin/Rscript", "Rscript")

  val picardJar: String =
    fileutils.TempFile
      .getExecutableFromJar("/bin/picard_2.8.14.jar", "picard_2.8.14.jar")
      .getAbsolutePath

  val umiProcessorJar: String =
    fileutils.TempFile
      .getExecutableFromJar("/umiprocessor", "umiprocessor")
      .getAbsolutePath

  val bwaExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/bwa_0.7.17-r1188_mac"
      else if (util.isLinux) "/bin/bwa_0.7.17-r1188_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "bwa_0.7.17-r1188")
      .getAbsolutePath
  }

  val bcftoolsExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/bcftools_1.9_mac"
      else if (util.isLinux) "/bin/bcftools_1.9_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "bcftools_1.9")
      .getAbsolutePath
  }

  val samtoolsExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/samtools_1.9_mac"
      else if (util.isLinux) "/bin/samtools_1.9_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))

    fileutils.TempFile
      .getExecutableFromJar(resourceName, "samtools_1.9")
      .getAbsolutePath

  }

  val fastpExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/fastp_v0.19.4_mac"
      else if (util.isLinux) "/bin/fastp_v0.19.4_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "fastp")
      .getAbsolutePath
  }
  val readQCJar: String =
    fileutils.TempFile
      .getExecutableFromJar("/readqc", "readqc")
      .getAbsolutePath

  val star261cExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/STAR_ffd8416315_2.6.1c_mac"
      else if (util.isLinux) "/bin/STAR_ffd8416315_2.6.1c_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "STAR_ffd8416315_2.6.1c")
      .getAbsolutePath
  }

  val star261aExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/STAR_45f7bd7_2.6.1a_mac"
      else if (util.isLinux) "/bin/STAR_45f7bd7_2.6.1a_linux64"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "STAR_45f7bd7_2.6.1a")
      .getAbsolutePath
  }

  val qtlToolsExecutable: String = {
    val resourceName =
      if (util.isMac) "/bin/QTLtools_9954dd57b36671a3_mac"
      else if (util.isLinux) "/bin/QTLtools_9954dd57b36671a3_linux"
      else
        throw new RuntimeException(
          "Unknown OS: " + System.getProperty("os.name"))
    fileutils.TempFile
      .getExecutableFromJar(resourceName, "QTLtools_9954dd57b36671a3")
      .getAbsolutePath
  }
}
