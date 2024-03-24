package org.saled
package data.structures.table

import data.types.DataTypes
import variables.TestingVariables.TESTING_VAR_TUPLE_ELEMENT

import org.scalatest.funsuite.AnyFunSuiteLike

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

class TupleTest extends AnyFunSuiteLike{
  test("TupleBuilderDefault") {
    val tuple = TupleBuilder().build()
    assert(tuple.tuple.isEmpty)
  }

  test("TupleBuilderValue") {
    val column = ColumnBuilder().withColumnName("test").withDatatype(DataTypes.String).build()
    val tupleElement = TESTING_VAR_TUPLE_ELEMENT
    val tuple = TupleBuilder().withTuple(List((column, tupleElement)).par.toMap).build()
    assert(tuple.tuple.nonEmpty)
  }
}
