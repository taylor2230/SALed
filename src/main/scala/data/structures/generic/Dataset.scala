package org.saled
package data.structures.generic

import data.structures.behaviors.{
  ColumnBehavior,
  FilterBehavior,
  SelectBehavior
}
import data.structures.table.{ColumnData, ColumnDefinition, Row}

trait Dataset
    extends DatasetStructure
    with ColumnBehavior
    with FilterBehavior
    with SelectBehavior {
  def display(limit: Int = 100, prettyPrint: Boolean = false): Unit = {
    val printLen: Int = 10
    val tableLimit: List[Row] =
      dataFrame.take(Math.min(100, Math.max(limit, 2)))

    val orderedHeader =
      dataFrameSchema.schema.map((c: ColumnDefinition) => c.toString)

    println(
      orderedHeader
        .map((k: String) => s"$k${" " * (printLen - k.length)}")
        .mkString(s" | ")
        .format(1)
    )

    tableLimit.foreach((row: Row) => {
      if (row.row.nonEmpty) {
        val adjustedRow = {
          row.row
            .map((r: (String, ColumnData)) => {
              (r._1, r._2)
            })
        }

        val orderedRow: List[String] = orderedHeader.map((c: String) => {
          val v: String = adjustedRow.getOrElse(c, "Null").toString
          val substringLength: Int = Math.min(v.length, printLen)
          s"${v.substring(0, substringLength)}${"." * (printLen - substringLength)}"
            .format(1)
        })

        println(
          orderedRow.mkString(" | ")
        )
      }
    })
  }
}
