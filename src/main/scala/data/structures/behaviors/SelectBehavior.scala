package org.saled
package data.structures.behaviors

import data.structures.generic.DatasetStructure
import data.structures.table.*

import scala.collection.parallel.immutable.ParMap

trait SelectBehavior extends DatasetStructure {
  def select(cols: String*): DataFrame = {
    import org.saled.data.implicits.Conversions.seqToSet
    val columnsSet: Set[String] = seqToSet(cols)
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
