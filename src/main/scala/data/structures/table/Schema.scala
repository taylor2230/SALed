package org.saled
package data.structures.table

import data.builder.Builder
import data.structures.table.SchemaDDL.createSchema
import data.types.{DataType, DataTypes}

import scala.collection.immutable.ListMap
import scala.collection.parallel.immutable.{ParMap, ParSeq}

case class Schema(schema: List[ColumnDefinition] = List.empty) {
  def getSchemaStrings: List[String] = {
    schema.map((col: ColumnDefinition) => {
      col.columnName
    })
  }

  override def toString: String = {
    schema
      .map((c: ColumnDefinition) => {
        s"${c.columnName} ${c.dataType.toString}"
      })
      .mkString(", ")
  }
}

case class SchemaBuilder(schema: List[ColumnDefinition] = List.empty)
    extends Builder[Schema] {

  def withSchema(schema: String): SchemaBuilder = {
    copy(schema = createSchema(schema))
  }

  def withSchema(schema: List[ColumnDefinition]): SchemaBuilder =
    copy(schema = schema)

  override def build(): Schema = Schema(schema = schema)
}

object SchemaDDL:
  private def getDatatype(dataTypeString: String): DataType[?] = {
    dataTypeString match {
      case DataTypes.Integer.datatypeDefinition    => DataTypes.Integer
      case DataTypes.Double.datatypeDefinition     => DataTypes.Double
      case DataTypes.Float.datatypeDefinition      => DataTypes.Float
      case DataTypes.BigInteger.datatypeDefinition => DataTypes.BigInteger
      case DataTypes.Boolean.datatypeDefinition    => DataTypes.Boolean
      case DataTypes.String.datatypeDefinition     => DataTypes.String
      case DataTypes.List.datatypeDefinition       => DataTypes.List
      case DataTypes.Map.datatypeDefinition        => DataTypes.Map
      case _                                       => DataTypes.String
    }
  }

  private val ddlStringToColumns: String => List[ColumnDefinition] =
    (ddlString: String) => {
      val parsedDDLStringToColumn
          : ListMap[String, DataType[?]] => List[ColumnDefinition] =
        (ddlMap: ListMap[String, DataType[?]]) => {
          ddlMap
            .map((rawColumn: (String, DataType[?])) => {
              ColumnDefinition(rawColumn._1, rawColumn._2)
            })
            .toList
        }

      val parsedDDL: ListMap[String, DataType[?]] = parseDDL(ddlString)
      parsedDDLStringToColumn(parsedDDL)
    }

  private def parseDDL(ddlString: String): ListMap[String, DataType[?]] = {
    val tupledDDL = ddlString
      .split(",")
      .map((columnDDL: String) => {
        val columnDefinition = columnDDL.trim.split(" ")
        columnDefinition.head -> getDatatype(columnDefinition.last)
      })
    ListMap(tupledDDL*)
  }

  def createSchema(ddlString: String): List[ColumnDefinition] = {
    if (ddlString.nonEmpty && !ddlString.isBlank) {
      val schema: List[ColumnDefinition] = {
        ddlStringToColumns(ddlString)
      }
      println(
        s"TableSchema:\n${schema
            .map((c: ColumnDefinition) => { s"${c.toString} (${c.dataType})" })
            .mkString(" | ")}\n".format(1)
      )
      schema
    } else {
      List.empty
    }
  }

  def inferSchemaDDL(columnSize: Int): List[ColumnDefinition] = {
    val inferredDDL: String = {
      val columnRanges: List[Int] = List.range(0, columnSize)
      columnRanges
        .map((c: Int) => {
          s"col_$c String"
        })
        .mkString(", ")
    }

    val schema: List[ColumnDefinition] = {
      createSchema(inferredDDL)
    }

    schema
  }

  def inferJsonSchemaDDL(
      json: ParSeq[Map[String, Any]]
  ): List[ColumnDefinition] = {
    val jsonKeys: ParSeq[(String, Option[DataType[?]])] =
      json.flatMap((r: Map[String, Any]) => {
        r.map((column: (String, Any)) => {
          (column._1, DataTypes.getDatatype(Option(column._2)))
        })
      })

    jsonKeys
      .map((col: (String, Option[DataType[?]])) => {
        if (col._2.nonEmpty) {
          Some(
            ColumnDefinitionBuilder()
              .withColumnName(col._1)
              .withDatatype(col._2.get)
              .build()
          )
        } else {
          None
        }
      })
      .filter((c: Option[ColumnDefinition]) => { c.nonEmpty })
      .map((c: Option[ColumnDefinition]) => { c.get })
      .toList
  }
