package org.saled
package data.structures.generic

import data.structures.table.{Column, Tuple, TupleElement}

trait Table extends TableStructure {
  def listToSet(columns: Seq[String]): Set[String] = {
    columns.toSet
  }

  def display(limit: Int = 100, prettyPrint: Boolean = false): Unit = {
    val printLen: Int = 10
    val tableLimit: List[Tuple] = table.take(Math.min(100, Math.max(limit, 2)))

    val orderedHeader = tableSchema.schema.map((c: Column) => c.toString)

    println(
      orderedHeader
        .map((k: String) => s"$k${" " * (printLen - k.length)}")
        .mkString(s" | ")
        .format(1)
    )

    tableLimit.foreach((row: Tuple) => {
      if (row.tuple.nonEmpty) {
        val adjustedRow = {
          row.tuple
            .map((r: (Column, TupleElement)) => {
              (r._1.toString, r._2)
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
