package org.saled
package data.structures.behaviors

import data.structures.generic.DatasetStructure
import data.structures.table._
import data.types.{DataType, DataTypes}

import scala.collection.parallel.immutable.ParMap

trait ColumnBehavior extends DatasetStructure {
  private def executeExpression(
                                 column: (String, ColumnData),
                                 expression: _ => Option[Any]
  ): Option[Any] = {
    if (column._2.data.nonEmpty) {
      val typeCastedValue = column._2.data.get
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

  def castColumnDataType(col: String, datatype: DataType[_]): DataFrame = {
    try {
      val result: List[Row] = dataFrame.map((row: Row) => {
        val updatedRow: ParMap[String, ColumnData] =
          row.row.map((c: (String, ColumnData)) => {
            if (c._1 == col) {
              val castedValue: Option[Any] =
                datatype.typeCast(c._2.data)
              val column: ColumnDefinition = ColumnDefinitionBuilder()
                .withColumnName(col)
                .withDatatype(DataTypes.getDatatype(castedValue).get)
                .build()
              (
                column.columnName,
                ColumnDataBuilder().withColumnData(castedValue).build()
              )
            } else {
              c
            }
          })
        RowBuilder().withRow(updatedRow).build()
      })

      val updatedSchema: Schema = {
        val castedSchema: List[ColumnDefinition] = dataFrameSchema.schema.map((c: ColumnDefinition) => {
          if (c.columnName == col) {
            ColumnDefinitionBuilder().withColumnName(col).withDatatype(datatype).build()
          } else {
            c
          }
        })

        SchemaBuilder().withSchema(castedSchema).build()
      }

      DataFrameBuilder()
        .withSchema(tableSchema = updatedSchema)
        .withData(result)
        .build()
    } catch {
      case _: Exception =>
        println("Failed type cast due to invalid value in TableSet");
        DataFrameBuilder()
          .withSchema(tableSchema = dataFrameSchema)
          .withData(dataFrame)
          .build()
    }
  }

  def dropColumns(cols: String*): DataFrame = {
    val filteredTuples: List[Row] = dataFrame.map((r: Row) => {
      val filteredRowTuples: ParMap[String, ColumnData] = r.row
        .filterKeys((c: String) => {
          !cols.contains(c)
        })
        .toMap
      RowBuilder().withRow(filteredRowTuples).build()
    })

    val filteredSchema: Schema = {
      val filteredSchema: List[ColumnDefinition] =
        dataFrameSchema.schema.filter((c: ColumnDefinition) => {
          !cols.contains(c.columnName)
        })

      SchemaBuilder().withSchema(filteredSchema).build()
    }

    DataFrameBuilder()
      .withSchema(filteredSchema)
      .withData(filteredTuples)
      .build()
  }

  def dropColumns(cols: List[String]): DataFrame = {
    dropColumns(cols: _*)
  }

  def dropColumn(col: String): DataFrame = {
    dropColumns(List(col))
  }

  def withNewColumn(
      col: String,
      newCol: String,
      datatype: DataType[_],
      expression: _ => Option[Any]
  ): DataFrame = {
    val newColumnDefinition: ColumnDefinition =
      ColumnDefinitionBuilder().withColumnName(newCol).withDatatype(datatype).build()
    val result: List[Row] = dataFrame.map((row: Row) => {
      val targetColumn =
        row.row.filterKeys((p: String) => p == col)
      val expressionColumn: ParMap[String, ColumnData] = targetColumn
        .map((c: (String, ColumnData)) => {
          val expressionResult: Option[Any] = executeExpression(c, expression)
          (
            newColumnDefinition.columnName,
            ColumnDataBuilder().withColumnData(expressionResult).build()
          )
        })
        .toMap

      val newRow: ParMap[String, ColumnData] = {
        if (col == newCol) {
          row.row
            .filterKeys((c: String) => c != newCol)
            .toMap ++ expressionColumn
        } else {
          row.row
        }
      }

      RowBuilder().withRow(newRow ++ expressionColumn).build()
    })

    val updatedSchema: List[ColumnDefinition] = {
      if (col == newCol) {
        dataFrameSchema.schema.filter((c: ColumnDefinition) =>
          c.columnName != newCol
        ) ++ List(newColumnDefinition)
      } else {
        dataFrameSchema.schema ++ List(newColumnDefinition)
      }
    }

    DataFrameBuilder()
      .withSchema(tableSchema =
        SchemaBuilder().withSchema(updatedSchema).build()
      )
      .withData(result)
      .build()
  }

  private def explodeArray(explodeCol: String): DataFrame = {
    val explodedTuples: List[Row] = dataFrame.flatMap((r: Row) => {
      val column: Option[ColumnData] = r.row.get(explodeCol)

      if (column.nonEmpty) {
        val columnValue: Option[Any] = column.get.data
        if (columnValue.get != None) {
          val columnList = column.get.data.get.asInstanceOf[List[_]]
          val explodedTuple = columnList.map((e: Any) => {
            val newTuple = ColumnDataBuilder().withColumnData(Some(e)).build()
            RowBuilder().withRow(r.row ++ ParMap((explodeCol, newTuple))).build()
          })
          explodedTuple
        } else {
          List(r)
        }
      } else {
        List(r)
      }
    })

    val updatedTableSchema: Schema = {
      val schema = dataFrameSchema.schema.map((column: ColumnDefinition) => {
        if (column.columnName == explodeCol) {
          ColumnDefinitionBuilder().withColumnName(explodeCol).withDatatype(DataTypes.String).build()
        } else {
          column
        }
      })
      SchemaBuilder().withSchema(schema).build()
    }

    DataFrameBuilder()
      .withData(table = explodedTuples)
      .withSchema(tableSchema = updatedTableSchema)
      .build()

  }

  def explodeColumn(explodeCol: String): DataFrame = {
    val columnSchema: Option[ColumnDefinition] =  dataFrameSchema.schema.find((p: ColumnDefinition) => {
      p.columnName == explodeCol
    })

    if (columnSchema.nonEmpty) {
      columnSchema.get.dataType match {
        case x: DataTypes.List.type => explodeArray(explodeCol)
        case _ =>
          DataFrameBuilder()
            .withData(table = dataFrame)
            .withSchema(tableSchema = dataFrameSchema)
            .build()
      }
    } else {
      DataFrameBuilder()
        .withData(table = dataFrame)
        .withSchema(tableSchema = dataFrameSchema)
        .build()
    }
  }
}
