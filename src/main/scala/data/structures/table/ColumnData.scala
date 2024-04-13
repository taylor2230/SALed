package org.saled
package data.structures.table

import data.builder.Builder

case class ColumnData(data: Option[Any] = None) {
  override def toString: String = {
    s"""${data.getOrElse("Null")}"""
  }
}

case class ColumnDataBuilder private(data: Option[Any] = None)
    extends Builder[ColumnData] {

  def withColumnData(data: Option[Any]): ColumnDataBuilder =
    copy(data = data)

  override def build(): ColumnData = ColumnData(data = data)
}
