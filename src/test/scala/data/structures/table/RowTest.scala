package org.saled
package data.structures.table

import data.types.DataTypes
import variables.TestingVariables.TESTING_VAR_TUPLE_ELEMENT

import org.scalatest.funsuite.AnyFunSuiteLike

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

class RowTest extends AnyFunSuiteLike{
  test("TupleBuilderDefault") {
    val tuple = RowBuilder().build()
    assert(tuple.row.isEmpty)
  }

  test("TupleBuilderValue") {
    val column = ColumnDefinitionBuilder().withColumnName("test").withDatatype(DataTypes.String).build()
    val tupleElement = TESTING_VAR_TUPLE_ELEMENT
    val tuple = RowBuilder().withRow(List((column, tupleElement)).par.toMap).build()
    assert(tuple.row.nonEmpty)
  }
}
