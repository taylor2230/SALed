package org.saled
package data.structures.table

import variables.TestingVariables.{TESTING_VAR_TABLE_SCHEMA, TESTING_VAR_TUPLES}

import org.scalatest.funsuite.AnyFunSuiteLike

class DataFrameTest extends AnyFunSuiteLike {
  test("TableSetDefaultBuilder") {
    val ts = DataFrameBuilder().build()
    assert(ts.dataFrameSchema.schema.isEmpty)
    assert(ts.dataFrame.isEmpty)
  }

  test("TableSetValueBuilder") {
    val ts = DataFrameBuilder().withSchema(TESTING_VAR_TABLE_SCHEMA).withData(TESTING_VAR_TUPLES).build()
    assert(ts.dataFrameSchema.schema.nonEmpty)
    assert(ts.dataFrame.nonEmpty)
  }
}
