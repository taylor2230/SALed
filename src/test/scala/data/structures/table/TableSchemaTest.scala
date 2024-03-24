package org.saled
package data.structures.table

import variables.TestingVariables.{TESTING_VAR_INVALID_DDL_STRING, TESTING_VAR_VALID_DDL_STRING}

import org.scalatest.funsuite.AnyFunSuiteLike

class TableSchemaTest extends AnyFunSuiteLike {

  test("TableSchemaValidSchema") {
    val tableSchema = TableSchemaBuilder().withSchema(TESTING_VAR_VALID_DDL_STRING).build()
    assert(tableSchema.schema.nonEmpty)
  }

  test("TableSchemaInvalidSchema") {
    val tableSchema = TableSchemaBuilder().withSchema(TESTING_VAR_INVALID_DDL_STRING).build()
    assert(tableSchema.schema.isEmpty)
  }

}
