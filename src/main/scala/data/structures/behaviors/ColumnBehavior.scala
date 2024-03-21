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

  def dropColumns(cols: String*): TableSet = {
    val filteredTuples: List[Tuple] = table.map((r: Tuple) => {
      val filteredRowTuples: ParMap[Column, TupleElement] = r.tuple.filterKeys((c: Column) => {
        !cols.contains(c.columnName)
      }).toMap
      TupleBuilder().withTuple(filteredRowTuples).build()
    })

    val filteredSchema: TableSchema = {
      val filteredSchema: List[Column] = tableSchema.schema.filter((c: Column) => {
        !cols.contains(c.columnName)
      })

      TableSchemaBuilder().withSchema(filteredSchema).build()
    }

    TableSetBuilder().withTableSchema(filteredSchema).withTableSet(filteredTuples).build()
  }

  def dropColumns(cols: List[String]): TableSet = {
    dropColumns(cols: _*)
  }

  def dropColumn(col: String): TableSet = {
    dropColumns(col)
  }

  def withNewColumn(
      col: String,
      newCol: String,
      datatype: DataType[_],
      expression: _ => Option[Any]
  ): TableSet = {
    val newColumnDefinition: Column = ColumnBuilder().withColumnName(newCol).withDatatype(datatype).build()
    val result: List[Tuple] = table.map((row: Tuple) => {
      val targetColumn = row.tuple.filterKeys((p: Column) => p.columnName == col)
      val expressionColumn: ParMap[Column, TupleElement] = targetColumn
        .map((c: (Column, TupleElement)) => {
          val expressionResult: Option[Any] = executeExpression(c, expression)
          (
            newColumnDefinition,
            TupleElementBuilder().withTupleElement(expressionResult).build()
          )
        })
        .toMap

      val newRow: ParMap[Column, TupleElement] = {
        if (col == newCol) {
          row.tuple.filterKeys((c: Column) => c.columnName != newCol).toMap ++ expressionColumn
        } else {
          row.tuple
        }
      }

      TupleBuilder().withTuple(newRow ++ expressionColumn).build()
    })

    val updatedSchema: List[Column] = {
      if (col == newCol) {
        tableSchema.schema.filter((c: Column) => c.columnName != newCol) ++ List(newColumnDefinition)
      } else {
        tableSchema.schema ++ List(newColumnDefinition)
      }
    }

    TableSetBuilder()
      .withTableSchema(tableSchema = TableSchemaBuilder().withSchema(updatedSchema).build())
      .withTableSet(result)
      .build()
  }
}
