package org.saled
package data.structures.behaviors

import data.structures.generic.Table
import data.structures.table._
import data.types.{DataType, DataTypes}

import scala.collection.parallel.immutable.ParMap

trait ColumnBehavior extends Table {
  private def executeExpression(
      column: (String, TupleElement),
      expression: _ => Option[Any]
  ): Option[Any] = {
    if (column._2.tupleElement.nonEmpty) {
      val typeCastedValue = column._2.tupleElement.get
      if (expression.isInstanceOf[typeCastedValue.type => Option[Any]]) {
        val passedExpression =
          expression.asInstanceOf[typeCastedValue.type => Option[Any]]
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
        val updatedRow: ParMap[String, TupleElement] =
          row.tuple.map((c: (String, TupleElement)) => {
            if (c._1 == col) {
              val castedValue: Option[Any] =
                datatype.typeCast(c._2.tupleElement)
              val column: Column = ColumnBuilder()
                .withColumnName(col)
                .withDatatype(DataTypes.getDatatype(castedValue).get)
                .build()
              (
                column.columnName,
                TupleElementBuilder().withTupleElement(castedValue).build()
              )
            } else {
              c
            }
          })
        TupleBuilder().withTuple(updatedRow).build()
      })

      val updatedSchema: TableSchema = {
        val castedSchema: List[Column] = tableSchema.schema.map((c: Column) => {
          if (c.columnName == col) {
            ColumnBuilder().withColumnName(col).withDatatype(datatype).build()
          } else {
            c
          }
        })

        TableSchemaBuilder().withSchema(castedSchema).build()
      }

      TableSetBuilder()
        .withTableSchema(tableSchema = updatedSchema)
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
      val filteredRowTuples: ParMap[String, TupleElement] = r.tuple
        .filterKeys((c: String) => {
          !cols.contains(c)
        })
        .toMap
      TupleBuilder().withTuple(filteredRowTuples).build()
    })

    val filteredSchema: TableSchema = {
      val filteredSchema: List[Column] =
        tableSchema.schema.filter((c: Column) => {
          !cols.contains(c.columnName)
        })

      TableSchemaBuilder().withSchema(filteredSchema).build()
    }

    TableSetBuilder()
      .withTableSchema(filteredSchema)
      .withTableSet(filteredTuples)
      .build()
  }

  def dropColumns(cols: List[String]): TableSet = {
    dropColumns(cols: _*)
  }

  def dropColumn(col: String): TableSet = {
    dropColumns(List(col))
  }

  def withNewColumn(
      col: String,
      newCol: String,
      datatype: DataType[_],
      expression: _ => Option[Any]
  ): TableSet = {
    val newColumnDefinition: Column =
      ColumnBuilder().withColumnName(newCol).withDatatype(datatype).build()
    val result: List[Tuple] = table.map((row: Tuple) => {
      val targetColumn =
        row.tuple.filterKeys((p: String) => p == col)
      val expressionColumn: ParMap[String, TupleElement] = targetColumn
        .map((c: (String, TupleElement)) => {
          val expressionResult: Option[Any] = executeExpression(c, expression)
          (
            newColumnDefinition.columnName,
            TupleElementBuilder().withTupleElement(expressionResult).build()
          )
        })
        .toMap

      val newRow: ParMap[String, TupleElement] = {
        if (col == newCol) {
          row.tuple
            .filterKeys((c: String) => c != newCol)
            .toMap ++ expressionColumn
        } else {
          row.tuple
        }
      }

      TupleBuilder().withTuple(newRow ++ expressionColumn).build()
    })

    val updatedSchema: List[Column] = {
      if (col == newCol) {
        tableSchema.schema.filter((c: Column) =>
          c.columnName != newCol
        ) ++ List(newColumnDefinition)
      } else {
        tableSchema.schema ++ List(newColumnDefinition)
      }
    }

    TableSetBuilder()
      .withTableSchema(tableSchema =
        TableSchemaBuilder().withSchema(updatedSchema).build()
      )
      .withTableSet(result)
      .build()
  }

  private def explodeArray(explodeCol: String): TableSet = {
    val explodedTuples: List[Tuple] = table.flatMap((r: Tuple) => {
      val column: Option[TupleElement] = r.tuple.get(explodeCol)

      if (column.nonEmpty) {
        val columnValue: Option[Any] = column.get.tupleElement
        if (columnValue.get != None) {
          val columnList = column.get.tupleElement.get.asInstanceOf[List[_]]
          val explodedTuple = columnList.map((e: Any) => {
            val newTuple = TupleElementBuilder().withTupleElement(Some(e)).build()
            TupleBuilder().withTuple(r.tuple ++ ParMap((explodeCol, newTuple))).build()
          })
          explodedTuple
        } else {
          List(r)
        }
      } else {
        List(r)
      }
    })

    val updatedTableSchema: TableSchema = {
      val schema = tableSchema.schema.map((column: Column) => {
        if (column.columnName == explodeCol) {
          ColumnBuilder().withColumnName(explodeCol).withDatatype(DataTypes.String).build()
        } else {
          column
        }
      })
      TableSchemaBuilder().withSchema(schema).build()
    }

    TableSetBuilder()
      .withTableSet(table = explodedTuples)
      .withTableSchema(tableSchema = updatedTableSchema)
      .build()

  }

  def explodeColumn(explodeCol: String): TableSet = {
    val columnSchema: Option[Column] =  tableSchema.schema.find((p: Column) => {
      p.columnName == explodeCol
    })

    if (columnSchema.nonEmpty) {
      columnSchema.get.dataType match {
        case x: DataTypes.List.type => explodeArray(explodeCol)
        case _ =>
          TableSetBuilder()
            .withTableSet(table = table)
            .withTableSchema(tableSchema = tableSchema)
            .build()
      }
    } else {
      TableSetBuilder()
        .withTableSet(table = table)
        .withTableSchema(tableSchema = tableSchema)
        .build()
    }
  }
}
