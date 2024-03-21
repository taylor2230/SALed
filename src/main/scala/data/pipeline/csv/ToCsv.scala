package org.saled
package data.pipeline.csv

import data.structures.table.{Column, TableSet, Tuple, TupleElement}

import java.io.{File, FileWriter}

trait ToCsv {
  def toCsv(tableSet: TableSet, csvOptions: CsvOptions, directory: String): Unit = {
    val filePath: String = s"$directory/csv_${System.currentTimeMillis()}.csv"
    val fileWriter: FileWriter = new FileWriter(new File(filePath))
    val orderedHeader = tableSet.tableSchema.schema.map((c: Column) => c.toString)

    fileWriter.write(s"${orderedHeader.mkString(s"${csvOptions.hasSeparator.get}")}\n")
    tableSet.table.foreach((row: Tuple) => {
      if (row.tuple.nonEmpty) {
        val adjustedRow = {
          row.tuple
            .map((r: (Column, TupleElement)) => {
              (r._1.toString, r._2)
            })
        }

        val orderedRow: String = orderedHeader.map((c: String) => {
          adjustedRow.getOrElse(c, "Null").toString
        }).mkString(s"${csvOptions.hasSeparator.get}")
        fileWriter.write(s"$orderedRow\n")
      }
    })

    fileWriter.close()
    println(s"CSV saved to $filePath")
  }
}
