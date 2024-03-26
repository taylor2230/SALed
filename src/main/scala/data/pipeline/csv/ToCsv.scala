package org.saled
package data.pipeline.csv

import data.structures.table.{Column, TableSet, Tuple, TupleElement}

import java.io.{File, FileWriter}
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

trait ToCsv {
  private val csvName: (String, String, Boolean) => String =
    (directory: String, fileName: String, includedTimestamp: Boolean) => {
      val timestamp: String = {
        if (includedTimestamp) {
          s"_${System.currentTimeMillis().toString}"
        } else {
          ""
        }
      }

      s"$directory/$fileName$timestamp.csv"
    }

  def toLocalCsv(
      tableSet: TableSet,
      csvOptions: CsvOptions,
      directory: String,
      fileName: String,
      includedTimestamp: Boolean
  ): Unit = {
    val filePath: String = csvName(directory, fileName, includedTimestamp)
    val fileWriter: FileWriter = new FileWriter(new File(filePath))
    val orderedHeader =
      tableSet.tableSchema.schema.map((c: Column) => c.toString)

    fileWriter.write(
      s"${orderedHeader.mkString(s"${csvOptions.hasSeparator.get}")}\n"
    )
    tableSet.table.par.foreach((row: Tuple) => {
      if (row.tuple.nonEmpty) {
        val adjustedRow = {
          row.tuple
            .map((r: (String, TupleElement)) => {
              (r._1.toString, r._2)
            })
        }

        val orderedRow: String = orderedHeader
          .map((c: String) => {
            adjustedRow.getOrElse(c, "Null").toString
          })
          .mkString(s"${csvOptions.hasSeparator.get}")
        fileWriter.write(s"$orderedRow\n")
      }
    })

    fileWriter.close()
    println(s"CSV saved to $filePath")
  }

  def toGCSCsv(
      tableSet: TableSet,
      csvOptions: CsvOptions,
      bucket: String,
      directory: String,
      fileName: String,
      includedTimestamp: Boolean
  ): Unit = {}
}
