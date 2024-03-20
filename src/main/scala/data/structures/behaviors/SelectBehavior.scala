package org.saled
package data.structures.behaviors

import org.saled.data.structures.generic.Table
import org.saled.data.structures.table.{Column, TableSchema, TableSchemaBuilder, TableSet, TableSetBuilder, Tuple, TupleBuilder, TupleElement}

import scala.collection.parallel.immutable.ParMap

trait SelectBehavior extends Table {
  def select(cols: String*): TableSet = {
    val columnsSet: Set[String] = listToSet(cols)
    val result: List[Tuple] = table.map((row: Tuple) => {
      val filteredTuple: ParMap[Column, TupleElement] = row.tuple.par
        .filterKeys((column: Column) => columnsSet.contains(column.columnName))
        .toMap
      TupleBuilder().withTuple(filteredTuple).build()
    })

    val filterColumns: List[Column] = tableSchema.schema.filter((c: Column) =>
      columnsSet.contains(c.columnName)
    )

    val newTableSchema: TableSchema =
      TableSchemaBuilder().withSchema(filterColumns).build()
    TableSetBuilder()
      .withTableSchema(newTableSchema)
      .withTableSet(result)
      .build()
  }

  def select(cols: List[String]): TableSet = {
    select(cols: _*)
  }

  def select(col: String): TableSet = {
    select(List(col))
  }
}
