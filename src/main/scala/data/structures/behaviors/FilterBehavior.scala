package org.saled
package data.structures.behaviors

import data.structures.generic.Table
import data.structures.table._

trait FilterBehavior extends Table {
  private def executePredicate(
      column: (Column, TupleElement),
      predicate: _ => Boolean
  ): Boolean = {
    if (column._2.tupleElement.nonEmpty) {
      try {
        val typeCastedValue = column._1.dataType.typeCast(column._2.tupleElement).get
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
      val filteredTuple = t.tuple.filter((col: (Column, TupleElement)) => {
        columnsSet.contains(col._1.columnName) && col._2.tupleElement.isEmpty
      })
      filteredTuple.size < tableSchema.schema.size
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
      val filteredTuple = t.tuple.filter((col: (Column, TupleElement)) => {
        columnsSet.contains(col._1.columnName) && col._2.tupleElement.nonEmpty
      })
      filteredTuple.size < tableSchema.schema.size
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
    val result: List[Tuple] = table.filter((t: Tuple) => {
      val passFilter = t.tuple
        .filterKeys((r: Column) => r.columnName == col)
        .filter((r: (Column, TupleElement)) => executePredicate(r, predicate))
      passFilter.nonEmpty
    })

    TableSetBuilder()
      .withTableSchema(tableSchema = tableSchema)
      .withTableSet(result)
      .build()
  }
}
