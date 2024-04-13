package org.saled
package data.structures.table

import data.builder.Builder

import scala.collection.parallel.immutable.ParMap

case class Row(row: ParMap[String, ColumnData] = ParMap.empty) {
  override def toString: String = {
    val rowString: String = row.map((t: (String, ColumnData)) => {
      s"""${t._1}: ${t._2.data.getOrElse("Null")}"""
    }).mkString(", ")
    rowString
  }
}

case class RowBuilder private(
                               row: ParMap[String, ColumnData] = ParMap.empty
) extends Builder[Row] {

  def withRow(row: ParMap[String, ColumnData]): RowBuilder = {
    if (row.nonEmpty) {
      copy(row = row)
    } else {
      copy(ParMap.empty)
    }
  }

  override def build(): Row = Row(row = row)
}
