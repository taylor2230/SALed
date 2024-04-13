package org.saled
package data.structures.table

import data.builder.Builder
import data.structures.generic.Dataset

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.immutable.ParMap

case class DataFrame(dataFrameSchema: Schema, dataFrame: List[Row])
    extends Dataset {}

case class DataFrameBuilder private(
                                     schema: Schema = SchemaBuilder().build(),
                                     data: List[Row] = List.empty
) extends Builder[DataFrame] {

  def withSchema(tableSchema: Schema): DataFrameBuilder =
    copy(schema = tableSchema)

  def withData(table: List[Row]): DataFrameBuilder = {
    copy(data = table)
  }

  override def build(): DataFrame = DataFrame(
    dataFrameSchema = schema,
    dataFrame = data
  )
}

object ToDataFrame {
  def createDataFrame(data: List[List[_]], schema: Schema): DataFrame = {
    val tableTuples: List[Row] = data.map((r: List[_]) => {
      val zippedRow: List[(String, Any)] = {
        for ((column, row) <- schema.schema zip r) yield (column.columnName, row)
      }


      val tupledRows: ParMap[String, ColumnData] = zippedRow.par
        .map((row: (String, Any)) => {
          val tupleElement: ColumnData =
            ColumnDataBuilder().withColumnData(Option(row._2)).build()
          (row._1, tupleElement)
        })
        .toMap

      val missingTuples: ParMap[String, ColumnData] =
        schema.schema.filter((c: ColumnDefinition) => !tupledRows.contains(c.columnName)).map((c: ColumnDefinition) => {
          (c.columnName, ColumnDataBuilder().withColumnData(None).build())
        }).par.toMap

      RowBuilder().withRow(tupledRows ++ missingTuples).build()
    })

    DataFrameBuilder()
      .withSchema(schema)
      .withData(tableTuples)
      .build()
      .select(schema.getSchemaStrings)
  }
}
