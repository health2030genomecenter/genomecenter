package org.gc.pipelines.util

object Html {

  def mkHtmlTable(
      headerLeadingColumns: Seq[String],
      headerColumns: Seq[(String, Boolean)],
      data: Seq[Seq[(String, Boolean)]]
  ) = {
    val header = mkHeader(headerLeadingColumns, headerColumns)
    val lines = data.map(line)
    mkDocument(header, lines.mkString("\n"))
  }

  def mkHeader(elems1: Seq[String], elems: Seq[(String, Boolean)]) =
    s"""
          <thead>
            <tr>
              ${elems1
      .dropRight(1)
      .map { elem =>
        s"""<th style="text-align: left; border-bottom: 1px solid #000; padding-right: 7px;">$elem</th>"""
      }
      .mkString("\n")}

          ${elems1.lastOption.map { elem =>
      s"""<th style="text-align: left; border-bottom: 1px solid #000; padding-right:30px;">$elem</th>"""
    }.mkString}

              ${elems
      .map {
        case (elem, left) =>
          val align = if (left) "left" else "right"
          s"""<th style="text-align: $align; border-bottom: 1px solid #000; padding-left: 7px;">$elem</th>"""
      }
      .mkString("\n")}
            </tr>
          </thead>
          """

  def line(elems: Seq[(String, Boolean)]) =
    s"""
          <tr>
          ${elems
      .map {
        case (elem, left) =>
          val align = if (left) "left" else "right"
          s"""<td style="text-align: $align">$elem</td>"""
      }
      .mkString("\n")}
          </tr>
          """

  def mkDocument(tableHeader: String, tableLines: String) =
    """<!DOCTYPE html><head></head><body><table style="border-collapse: collapse;">""" + tableHeader + "\n<tbody>" + tableLines + "</tbody></table></body>"

}
