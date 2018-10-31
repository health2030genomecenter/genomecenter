package org.gc.pipelines.util

import htsjdk.samtools.reference.{ReferenceSequenceFileFactory}
import htsjdk.samtools.SAMSequenceDictionary
import java.io.{File, InputStream}
object Fasta {

  def parseDict(f: File): SAMSequenceDictionary = {
    fileutils.openFileInputStream(f)(is => parseDict(is))
  }

  def parseDict(is: InputStream): SAMSequenceDictionary =
    ReferenceSequenceFileFactory.loadDictionary(is)

}
