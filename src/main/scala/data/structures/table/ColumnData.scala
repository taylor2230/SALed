package org.saled
package data.structures.table

import data.builder.Builder

case class ColumnData(data: Option[?] = None) {
  override def toString: String = {
    s"""${data.getOrElse("Null")}"""
  }
}

case class ColumnDataBuilder(data: Option[?] = None)
    extends Builder[ColumnData] {

  def withColumnData(data: Option[?]): ColumnDataBuilder =
    copy(data = data)

  override def build(): ColumnData = ColumnData(data = data)
}
