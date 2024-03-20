package org.saled
package data.structures.table

import data.builder.Builder
import data.types.{DataType, DataTypes}

case class Column(
    columnName: String = "",
    dataType: DataType[_] = DataTypes.String
) {
  override def toString: String = {
    columnName
  }
}

case class ColumnBuilder private (
    columnName: String = "",
    dataType: DataType[_] = DataTypes.String
) extends Builder[Column] {
  def withColumnName(columnName: String): ColumnBuilder = {
    copy(columnName = columnName)
  }

  def withDatatype(dataType: DataType[_]): ColumnBuilder = {
    copy(dataType = dataType)
  }

  override def build(): Column =
    Column(columnName = columnName, dataType = dataType)
}
