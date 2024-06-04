package org.saled
package data.structures.table

import data.builder.Builder
import data.structures.generic.Dataset

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.immutable.ParMap

case class DataFrame(
    dataFrameSchema: Schema = SchemaBuilder().build(),
    dataFrame: List[Row] = List.empty
) extends Dataset {
  def unionDF(df: DataFrame): DataFrame = {
    this
  }
}

case class DataFrameBuilder(
    schema: Schema = SchemaBuilder().build(),
    data: List[Row] = List.empty
) extends Builder[DataFrame] {

  def withSchema(tableSchema: Schema): DataFrameBuilder =
    copy(schema = tableSchema)

  def withData(table: List[Row]): DataFrameBuilder = {
    copy(data = table)
  }

  def empty(): DataFrame = {
    DataFrameBuilder().build()
  }

  override def build(): DataFrame = DataFrame(
    dataFrameSchema = schema,
    dataFrame = data
  )
}

object ToDataFrame:
  def createDataFrame(data: Seq[Seq[?]], schema: Schema): DataFrame = {
    val tableTuples: List[Row] = data
      .map((r: Seq[?]) => {
        val zippedRow: List[(String, Any)] = {
          for ((column, row) <- schema.schema zip r)
            yield (column.columnName, row)
        }

        val tupledRows: ParMap[String, ColumnData] = zippedRow.par
          .map((row: (String, Any)) => {
            val tupleElement: ColumnData =
              ColumnDataBuilder().withColumnData(Option(row._2)).build()
            (row._1, tupleElement)
          })
          .toMap

        val missingTuples: ParMap[String, ColumnData] =
          schema.schema
            .filter((c: ColumnDefinition) => !tupledRows.contains(c.columnName))
            .map((c: ColumnDefinition) => {
              (c.columnName, ColumnDataBuilder().withColumnData(None).build())
            })
            .par
            .toMap

        RowBuilder().withRow(tupledRows ++ missingTuples).build()
      })
      .toList

    DataFrameBuilder()
      .withSchema(schema)
      .withData(tableTuples)
      .build()
      .select(schema.getSchemaStrings)
  }
