package org.saled
package data.structures.table
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import data.types.{ArrowDataType, ArrowDataTypes}

import scala.collection.immutable.ListMap

class ArrowSchema {}

object ArrowSchemaDDL:
  private def getDatatype(dataTypeString: String): ArrowDataType[?] = {
    dataTypeString match {
      case ArrowDataTypes.Integer.definition => ArrowDataTypes.Integer
      case ArrowDataTypes.Decimal.definition => ArrowDataTypes.Decimal
      case ArrowDataTypes.BigInt.definition  => ArrowDataTypes.BigInt
      case ArrowDataTypes.Boolean.definition => ArrowDataTypes.Boolean
      case ArrowDataTypes.String.definition  => ArrowDataTypes.String
      case ArrowDataTypes.Struct.definition
          if dataTypeString.contains(ArrowDataTypes.Struct.definition) =>
        ArrowDataTypes.Struct
      case ArrowDataTypes.List.definition => ArrowDataTypes.List
      case ArrowDataTypes.Date.definition => ArrowDataTypes.Date
      case ArrowDataTypes.TimestampMs.definition =>
        ArrowDataTypes.TimestampMs
      case ArrowDataTypes.Timestamp.definition => ArrowDataTypes.Timestamp
      case _                                   => ArrowDataTypes.String
    }
  }

  private val ddlStringToColumns: String => List[Field] =
    (ddlString: String) => {
      val parsedDDLStringToColumn
          : ListMap[String, ArrowDataType[?]] => List[Field] =
        (ddlMap: ListMap[String, ArrowDataType[?]]) => {
          ddlMap
            .map((rawColumn: (String, ArrowDataType[?])) => {
              Field(
                rawColumn._1,
                FieldType.nullable(
                  rawColumn._2.arrowDataType.asInstanceOf[ArrowType]
                ),
                null
              )
            })
            .toList
        }

      val parsedDDL: ListMap[String, ArrowDataType[?]] = parseDDL(ddlString)
      parsedDDLStringToColumn(parsedDDL)
    }

  private def parseDDL(ddlString: String): ListMap[String, ArrowDataType[?]] = {
    val tupledDDL = ddlString
      .split(",")
      .map((columnDDL: String) => {
        val columnDefinition = columnDDL.trim.split(" ")
        columnDefinition.head -> getDatatype(columnDefinition.last)
      })
    ListMap(tupledDDL*)
  }

  def createSchema(ddlString: String): List[Field] = {
    if (ddlString.nonEmpty && !ddlString.isBlank) {
      val schema: List[Field] = {
        ddlStringToColumns(ddlString)
      }
      println(
        s"TableSchema:\n${schema
            .map((c: Field) => {
              s"${c.getName} (${c.getType})"
            })
            .mkString(" | ")}\n".format(1)
      )
      schema
    } else {
      List.empty
    }
  }

  def inferSchemaDDL(columnSize: Int): List[Field] = {
    val inferredDDL: String = {
      val columnRanges: List[Int] = List.range(0, columnSize)
      columnRanges
        .map((c: Int) => {
          s"col_$c Any"
        })
        .mkString(", ")
    }

    val schema: List[Field] = {
      createSchema(inferredDDL)
    }

    schema
  }
