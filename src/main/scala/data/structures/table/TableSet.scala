package org.saled
package data.structures.table

import data.builder.Builder
import data.structures.behaviors.{ColumnBehavior, FilterBehavior, SelectBehavior}
import data.structures.generic.Table

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.collection.parallel.immutable.ParMap

case class TableSet(tableSchema: TableSchema, table: List[Tuple])
    extends Table
    with SelectBehavior
    with ColumnBehavior
    with FilterBehavior {}

case class TableSetBuilder private (
    tableSchema: TableSchema = TableSchemaBuilder().build(),
    table: List[Tuple] = List.empty
) extends Builder[TableSet] {

  def withTableSchema(tableSchema: TableSchema): TableSetBuilder =
    copy(tableSchema = tableSchema)

  def withTableSet(table: List[Tuple]): TableSetBuilder = {
    copy(table = table)
  }

  override def build(): TableSet = TableSet(
    tableSchema = tableSchema,
    table = table
  )
}

object ToTableSet {
  def createTableSet(data: List[List[_]], schema: TableSchema): TableSet = {
    val tableTuples: List[Tuple] = data.map((r: List[_]) => {
      val zippedRow: List[(String, Any)] = {
        for ((column, row) <- schema.schema zip r) yield (column.columnName, row)
      }


      val tupledRows: ParMap[String, TupleElement] = zippedRow.par
        .map((row: (String, Any)) => {
          val tupleElement: TupleElement =
            TupleElementBuilder().withTupleElement(Option(row._2)).build()
          (row._1, tupleElement)
        })
        .toMap

      val missingTuples: ParMap[String, TupleElement] =
        schema.schema.filter((c: Column) => !tupledRows.contains(c.columnName)).map((c: Column) => {
          (c.columnName, TupleElementBuilder().withTupleElement(None).build())
        }).par.toMap

      TupleBuilder().withTuple(tupledRows ++ missingTuples).build()
    })

    TableSetBuilder()
      .withTableSchema(schema)
      .withTableSet(tableTuples)
      .build()
      .select(schema.getSchemaStrings)
  }
}
