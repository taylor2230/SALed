package org.saled
package data.pipeline.json

import data.structures.table.{ColumnDefinition, DataFrame, Row, ColumnData}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

trait ToJson {
  def toJson(tableSet: DataFrame): String = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val formattedString: List[(String, Any)] = {
      tableSet.dataFrame.flatMap((r: Row) => {
        r.row.map((col: (String, ColumnData)) => {
          (col._1, col._2.data.getOrElse("Null"))
        })
      })
    }

    write(formattedString)
  }
}
