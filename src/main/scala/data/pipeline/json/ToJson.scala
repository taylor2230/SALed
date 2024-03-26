package org.saled
package data.pipeline.json

import data.structures.table.{Column, TableSet, Tuple, TupleElement}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

trait ToJson {
  def toJson(tableSet: TableSet): String = {
    implicit val format: DefaultFormats.type = DefaultFormats
    val formattedString: List[(String, Any)] = {
      tableSet.table.flatMap((r: Tuple) => {
        r.tuple.map((col: (String, TupleElement)) => {
          (col._1, col._2.tupleElement.getOrElse("Null"))
        })
      })
    }

    write(formattedString)
  }
}
