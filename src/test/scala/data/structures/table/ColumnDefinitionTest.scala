package org.saled
package data.structures.table

import data.types.DataTypes

import org.scalatest.funsuite.AnyFunSuiteLike

class ColumnDefinitionTest extends AnyFunSuiteLike {
  test("ColumnBuilderDefault") {
    val column = ColumnDefinitionBuilder().build()
    assert(column.columnName == "" && column.dataType == DataTypes.String)
  }

  test("ColumnBuilderValue") {
    val column = ColumnDefinitionBuilder().withColumnName("test").withDatatype(DataTypes.Integer).build()
    assert(column.columnName == "test" && column.dataType == DataTypes.Integer)
  }
}
