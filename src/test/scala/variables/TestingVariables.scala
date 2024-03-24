package org.saled
package variables

import data.structures.table.{
  Column,
  TableSchema,
  TableSchemaBuilder,
  Tuple,
  TupleBuilder,
  TupleElement,
  TupleElementBuilder
}
import data.types.DataTypes

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

object TestingVariables {
  val TESTING_VAR_VALID_DDL_STRING: String = "col_1 String, col_2 Integer"
  val TESTING_VAR_INVALID_DDL_STRING: String = " "
  val TESTING_VAR_TABLE_SCHEMA: TableSchema =
    TableSchemaBuilder().withSchema(TESTING_VAR_VALID_DDL_STRING).build()
  val TESTING_VAR_COLUMN_SCHEMA: List[Column] = List(
    Column("col_1", DataTypes.Integer)
  )
  val TESTING_VAR_TUPLE_ELEMENT: TupleElement =
    TupleElementBuilder().withTupleElement(Some(1)).build()
  val TESTING_VAR_TUPLES: List[Tuple] = List(
    TupleBuilder()
      .withTuple(
        List(
          (TESTING_VAR_COLUMN_SCHEMA.head, TESTING_VAR_TUPLE_ELEMENT)
        ).par.toMap
      )
      .build()
  )
}
