package org.saled
package data.structures.table

import data.builder.Builder
import data.structures.table.TableSchemaDDL.createSchema
import data.types.{DataType, DataTypes}

import scala.collection.immutable.ListMap

case class TableSchema(schema: List[Column] = List.empty) {
  def getSchemaStrings: List[String] = {
    schema.map((col: Column) => {
      col.columnName
    })
  }

  override def toString: String = {
    schema.map((c: Column) => {s"${c.columnName} ${c.dataType.toString}"}).mkString(", ")
  }
}

case class TableSchemaBuilder private (schema: List[Column] = List.empty)
    extends Builder[TableSchema] {

  def withSchema(schema: String): TableSchemaBuilder = {
    copy(schema = createSchema(schema))
  }

  def withSchema(schema: List[Column]): TableSchemaBuilder =
    copy(schema = schema)

  override def build(): TableSchema = TableSchema(schema = schema)
}

object TableSchemaDDL {
  private def getDatatype(dataTypeString: String): DataType[_] = {
    dataTypeString match {
      case DataTypes.Integer.datatypeDefinition => DataTypes.Integer
      case DataTypes.String.datatypeDefinition  => DataTypes.String
      case _                                    => DataTypes.String
    }
  }

  private val ddlStringToColumns: String => List[Column] =
    (ddlString: String) => {
      val parsedDDLStringToColumn
          : ListMap[String, DataType[_]] => List[Column] =
        (ddlMap: ListMap[String, DataType[_]]) => {
          ddlMap
            .map((rawColumn: (String, DataType[_])) => {
              Column(rawColumn._1, rawColumn._2)
            })
            .toList
        }

      val parsedDDL: ListMap[String, DataType[_]] = parseDDL(ddlString)
      parsedDDLStringToColumn(parsedDDL)
    }

  private def parseDDL(ddlString: String): ListMap[String, DataType[_]] = {
    val tupledDDL = ddlString
      .split(",")
      .map((columnDDL: String) => {
        val columnDefinition = columnDDL.trim.split(" ")
        columnDefinition.head -> getDatatype(columnDefinition.last)
      })
    ListMap(tupledDDL: _*)
  }

  def createSchema(ddlString: String): List[Column] = {
    val schema: List[Column] = {
      ddlStringToColumns(ddlString)
    }
    println(
      s"TableSchema:\n${schema.map((c: Column) => { s"${c.toString} (${c.dataType})" }).mkString(" | ")}\n".format(1)
    )
    schema
  }

  def inferSchemaDDL(columnSize: Int): List[Column] = {
    val inferredDDL: String = {
      val columnRanges: List[Int] = List.range(0, columnSize)
      columnRanges.map((c: Int) => {
          s"col_$c String"
        }).mkString(", ")
    }

    val schema: List[Column] = {
      createSchema(inferredDDL)
    }

    schema
  }
}
