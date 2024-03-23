package org.saled
package data.structures.table

import variables.TestingVariables.{TESTING_VAR_TABLE_SCHEMA, TESTING_VAR_TUPLES}

import org.scalatest.funsuite.AnyFunSuiteLike

class TableSetTest extends AnyFunSuiteLike {
  test("TableSetDefaultBuilder") {
    val ts = TableSetBuilder().build()
    assert(ts.tableSchema.schema.isEmpty)
    assert(ts.table.isEmpty)
  }

  test("TableSetValueBuilder") {
    val ts = TableSetBuilder().withTableSchema(TESTING_VAR_TABLE_SCHEMA).withTableSet(TESTING_VAR_TUPLES).build()
    assert(ts.tableSchema.schema.nonEmpty)
    assert(ts.table.nonEmpty)
  }
}
