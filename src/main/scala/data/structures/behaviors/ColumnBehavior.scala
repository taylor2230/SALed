package org.saled
package data.structures.behaviors

import data.structures.generic.Table
import data.structures.table._
import data.types.DataType

import scala.collection.parallel.immutable.ParMap

trait ColumnBehavior extends Table {
  private def executeExpression(column: (Column, TupleElement), expression: _ => Option[Any]): Option[Any] = {
    if (column._2.tupleElement.nonEmpty) {
      val typeCastedValue = column._2.tupleElement.get
      if (expression.isInstanceOf[typeCastedValue.type => Option[Any]]) {
        val passedExpression = expression.asInstanceOf[typeCastedValue.type => Option[Any]]
        passedExpression(typeCastedValue)
      } else {
        None
      }
    } else {
      None
    }
  }


  def castColumnDataType(col: String, datatype: DataType[_]): TableSet = {
    try {
      val result: List[Tuple] = table.map((row: Tuple) => {
        val updatedRow: ParMap[Column, TupleElement] =
          row.tuple.map((c: (Column, TupleElement)) => {
            if (c._1.columnName == col) {
              datatype.typeCast(c._2.tupleElement)
              val column: Column = ColumnBuilder().withColumnName(col).withDatatype(datatype).build()
              (column, c._2)
            } else {
              c
            }
          })
        TupleBuilder().withTuple(updatedRow).build()
      })

      TableSetBuilder()
        .withTableSchema(tableSchema = tableSchema)
        .withTableSet(result)
        .build()
    } catch {
      case _: Exception =>
        println("Failed type cast due to invalid value in TableSet");
        TableSetBuilder()
          .withTableSchema(tableSchema = tableSchema)
          .withTableSet(table)
          .build()
    }
  }

  def withNewColumn(
      col: String,
      newCol: String,
      datatype: DataType[_],
      expression: _ => Option[Any]
  ): TableSet = {
    val newColumnDefinition: Column = ColumnBuilder().withColumnName(newCol).withDatatype(datatype).build()
    val result: List[Tuple] = table.map((row: Tuple) => {
      val targetColumn =
        row.tuple.filterKeys((p: Column) => p.columnName == col)
      val expressionColumn: ParMap[Column, TupleElement] = targetColumn
        .map((c: (Column, TupleElement)) => {
          val expressionResult: Option[Any] = executeExpression(c, expression)
          (
            newColumnDefinition,
            TupleElementBuilder().withTupleElement(expressionResult).build()
          )
        })
        .toMap

      TupleBuilder().withTuple(row.tuple ++ expressionColumn).build()
    })

    val updatedSchema: List[Column] = tableSchema.schema ++ List(newColumnDefinition)

    TableSetBuilder()
      .withTableSchema(tableSchema = TableSchemaBuilder().withSchema(updatedSchema).build())
      .withTableSet(result)
      .build()
  }
}
