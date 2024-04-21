package org.saled
package data.structures.table

import data.builder.Builder
import data.types.{DataType, DataTypes}

case class ColumnDefinition(
    columnName: String = "",
    dataType: DataType[?] = DataTypes.String
) {
  override def toString: String = {
    columnName
  }
}

case class ColumnDefinitionBuilder(
    columnName: String = "",
    dataType: DataType[?] = DataTypes.String
) extends Builder[ColumnDefinition] {
  def withColumnName(columnName: String): ColumnDefinitionBuilder = {
    copy(columnName = columnName)
  }

  def withDatatype(dataType: DataType[?]): ColumnDefinitionBuilder = {
    copy(dataType = dataType)
  }

  override def build(): ColumnDefinition =
    ColumnDefinition(columnName = columnName, dataType = dataType)
}
