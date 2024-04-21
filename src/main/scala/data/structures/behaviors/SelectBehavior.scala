package org.saled
package data.structures.behaviors

import data.structures.generic.DatasetStructure
import data.structures.table._

import scala.collection.parallel.immutable.ParMap

trait SelectBehavior extends DatasetStructure {
  def select(cols: String*): DataFrame = {
    val columnsSet: Set[String] = listToSet(cols)
    val result: List[Row] = dataFrame.map((row: Row) => {
      val filteredTuple: ParMap[String, ColumnData] = row.row.par
        .filterKeys((column: String) => columnsSet.contains(column))
        .toMap
      RowBuilder().withRow(filteredTuple).build()
    })

    val filterColumns: List[ColumnDefinition] = dataFrameSchema.schema.filter((c: ColumnDefinition) =>
      columnsSet.contains(c.columnName)
    )

    val newTableSchema: Schema =
      SchemaBuilder().withSchema(filterColumns).build()
    DataFrameBuilder()
      .withSchema(newTableSchema)
      .withData(result)
      .build()
  }

  def select(cols: List[String]): DataFrame = {
    select(cols*)
  }

  def select(col: String): DataFrame = {
    select(List(col))
  }
}
