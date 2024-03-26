package org.saled
package data.structures.behaviors

import data.structures.generic.Table
import data.structures.table._
import data.types.DataType

import scala.collection.parallel.immutable.ParMap

trait FilterBehavior extends Table {
  private def executePredicate(
      column: (String, TupleElement),
      dataType: DataType[_],
      predicate: _ => Boolean
  ): Boolean = {
    if (column._2.tupleElement.nonEmpty) {
      try {
        val typeCastedValue = dataType.typeCast(column._2.tupleElement).get
        if (predicate.isInstanceOf[typeCastedValue.type => Boolean]) {
          val passedPredicate: typeCastedValue.type => Boolean = predicate.asInstanceOf[typeCastedValue.type => Boolean]
          passedPredicate(typeCastedValue)
        } else {
          println("Invalid predicate execution due to mismatched input type")
          false
        }
      } catch {
        case x: Exception =>
          println(s"Failed predicate conversion; skipping predicate; ${x.getMessage}"); false;
      }
    } else {
      false
    }
  }

  def isNull(cols: String*): TableSet = {
    val columnsSet: Set[String] = listToSet(cols)
    val result: List[Tuple] = table.filter((t: Tuple) => {
      val nullColumnsPredicate = columnsSet.filter((c: String) => {
        t.tuple(c).tupleElement.isEmpty
      })
      nullColumnsPredicate.size == columnsSet.size
    })

    TableSetBuilder()
      .withTableSchema(tableSchema = tableSchema)
      .withTableSet(result)
      .build()
  }

  def isNull(cols: List[String]): TableSet = {
    isNull(cols: _*)
  }

  def isNull(col: String): TableSet = {
    isNull(List(col))
  }

  def nonNull(cols: String*): TableSet = {
    val columnsSet: Set[String] = listToSet(cols)
    val result: List[Tuple] = table.filter((t: Tuple) => {
      val nonNullColumnsPredicate = columnsSet.filter((c: String) => {
        t.tuple(c).tupleElement.nonEmpty
      })
      nonNullColumnsPredicate.size == columnsSet.size
    })

    TableSetBuilder()
      .withTableSchema(tableSchema = tableSchema)
      .withTableSet(result)
      .build()
  }

  def nonNull(cols: List[String]): TableSet = {
    nonNull(cols: _*)
  }

  def nonNull(col: String): TableSet = {
    nonNull(List(col))
  }

  def where(col: String, predicate: _ => Boolean): TableSet = {
    val columnDefinition: Option[Column] = tableSchema.schema.find((c: Column) => c.columnName == col)

    val tableSet: List[Tuple] = {
      if (columnDefinition.nonEmpty) {
        val result: List[Tuple] = table.filter((t: Tuple) => {
          val passFilter = t.tuple
            .filterKeys((r: String) => r == col)
            .filter((r: (String, TupleElement)) => executePredicate(r, columnDefinition.get.dataType, predicate))
          passFilter.nonEmpty
        })
        result
      } else {
        table
      }
    }

    TableSetBuilder()
      .withTableSchema(tableSchema = tableSchema)
      .withTableSet(tableSet)
      .build()
  }
}
