package org.gc.pipelines.util

object Csv {

  def mkHeader(elems1: Seq[String], elems: Seq[(String, Boolean)]) =
    (elems1 ++ elems.map(_._1)).mkString(",")

  def line(elems: Seq[(String, Boolean)]) = elems.map(_._1).mkString(",")

}
