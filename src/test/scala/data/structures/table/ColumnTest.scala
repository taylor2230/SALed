package org.saled
package data.structures.table

import data.types.DataTypes

import org.scalatest.funsuite.AnyFunSuiteLike

class ColumnTest extends AnyFunSuiteLike {
  test("ColumnBuilderDefault") {
    val column = ColumnBuilder().build()
    assert(column.columnName == "" && column.dataType == DataTypes.String)
  }

  test("ColumnBuilderValue") {
    val column = ColumnBuilder().withColumnName("test").withDatatype(DataTypes.Integer).build()
    assert(column.columnName == "test" && column.dataType == DataTypes.Integer)
  }
}
