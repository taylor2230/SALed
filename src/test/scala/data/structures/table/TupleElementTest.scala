package org.saled
package data.structures.table

import org.scalatest.funsuite.AnyFunSuiteLike

class TupleElementTest extends AnyFunSuiteLike {
  test("TupleElementBuilderDefault") {
    val tupleElement = TupleElementBuilder().build()
    assert(tupleElement.tupleElement.isEmpty)
  }

  test("TupleElementBuilderValue") {
    val tupleElement = TupleElementBuilder().withTupleElement(Some(1)).build()
    assert(tupleElement.tupleElement.nonEmpty)
    assert(tupleElement.tupleElement.get == 1)
  }

  test("TupleElementToString") {
    val tupleElement = TupleElementBuilder().withTupleElement(Some(1)).build()
    assert(tupleElement.tupleElement.nonEmpty)
    assert(tupleElement.tupleElement.get == 1)
    assert(tupleElement.tupleElement.toString != null)
  }
}
