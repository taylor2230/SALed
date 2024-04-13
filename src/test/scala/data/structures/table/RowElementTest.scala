package org.saled
package data.structures.table

import org.scalatest.funsuite.AnyFunSuiteLike

class RowElementTest extends AnyFunSuiteLike {
  test("TupleElementBuilderDefault") {
    val tupleElement = ColumnDataBuilder().build()
    assert(tupleElement.data.isEmpty)
  }

  test("TupleElementBuilderValue") {
    val tupleElement = ColumnDataBuilder().withColumnData(Some(1)).build()
    assert(tupleElement.data.nonEmpty)
    assert(tupleElement.data.get == 1)
  }

  test("TupleElementToString") {
    val tupleElement = ColumnDataBuilder().withColumnData(Some(1)).build()
    assert(tupleElement.data.nonEmpty)
    assert(tupleElement.data.get == 1)
    assert(tupleElement.data.toString != null)
  }
}
