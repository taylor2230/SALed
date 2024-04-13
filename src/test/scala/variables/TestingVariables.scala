package org.saled
package variables

import data.structures.table.{
  ColumnDefinition,
  Schema,
  SchemaBuilder,
  Row,
  RowBuilder,
  ColumnData,
  ColumnDataBuilder
}
import data.types.DataTypes

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable

object TestingVariables {
  val TESTING_VAR_VALID_DDL_STRING: String = "col_1 String, col_2 Integer"
  val TESTING_VAR_INVALID_DDL_STRING: String = " "
  val TESTING_VAR_TABLE_SCHEMA: Schema =
    SchemaBuilder().withSchema(TESTING_VAR_VALID_DDL_STRING).build()
  val TESTING_VAR_COLUMN_SCHEMA: List[ColumnDefinition] = List(
    ColumnDefinition("col_1", DataTypes.Integer)
  )
  val TESTING_VAR_TUPLE_ELEMENT: ColumnData =
    ColumnDataBuilder().withColumnData(Some(1)).build()
  val TESTING_VAR_TUPLES: List[Row] = List(
    RowBuilder()
      .withRow(
        List(
          (TESTING_VAR_COLUMN_SCHEMA.head, TESTING_VAR_TUPLE_ELEMENT)
        ).par.toMap
      )
      .build()
  )
}
