package org.saled
package data.structures.behaviors

import data.structures.generic.DatasetStructure
import data.structures.table.*
import data.types.DataType

trait FilterBehavior extends DatasetStructure {
  private def executePredicate(
      column: (String, ColumnData),
      dataType: DataType[?],
      predicate: ? => Boolean
  ): Boolean = {
    if (column._2.data.nonEmpty) {
      try {
        val typeCastedValue = dataType.typeCast(column._2.data).get
        if (predicate.isInstanceOf[typeCastedValue.type => Boolean]) {
          val passedPredicate: typeCastedValue.type => Boolean =
            predicate.asInstanceOf[typeCastedValue.type => Boolean]
          passedPredicate(typeCastedValue)
        } else {
          println("Invalid predicate execution due to mismatched input type")
          false
        }
      } catch {
        case x: Exception =>
          println(
            s"Failed predicate conversion; skipping predicate; ${x.getMessage}"
          ); false;
      }
    } else {
      false
    }
  }

  def isNull(cols: String*): DataFrame = {
    import org.saled.data.implicits.Conversions.seqToSet
    val columnsSet: Set[String] = seqToSet(cols)
    val result: List[Row] = dataFrame.filter((t: Row) => {
      val nullColumnsPredicate = columnsSet.filter((c: String) => {
        t.row(c).data.isEmpty
      })
      nullColumnsPredicate.size == columnsSet.size
    })

    DataFrameBuilder()
      .withSchema(tableSchema = dataFrameSchema)
      .withData(result)
      .build()
  }

  def isNull(cols: List[String]): DataFrame = {
    isNull(cols*)
  }

  def isNull(col: String): DataFrame = {
    isNull(List(col))
  }

  def nonNull(cols: String*): DataFrame = {
    import org.saled.data.implicits.Conversions.seqToSet
    val columnsSet: Set[String] = seqToSet(cols)
    val result: List[Row] = dataFrame.filter((t: Row) => {
      val nonNullColumnsPredicate = columnsSet.filter((c: String) => {
        t.row(c).data.nonEmpty
      })
      nonNullColumnsPredicate.size == columnsSet.size
    })

    DataFrameBuilder()
      .withSchema(tableSchema = dataFrameSchema)
      .withData(result)
      .build()
  }

  def nonNull(cols: List[String]): DataFrame = {
    nonNull(cols*)
  }

  def nonNull(col: String): DataFrame = {
    nonNull(List(col))
  }

  def where(col: String, predicate: ? => Boolean): DataFrame = {
    val columnDefinition: Option[ColumnDefinition] =
      dataFrameSchema.schema.find((c: ColumnDefinition) => c.columnName == col)

    val tableSet: List[Row] = {
      if (columnDefinition.nonEmpty) {
        val result: List[Row] = dataFrame.filter((t: Row) => {
          val passFilter = t.row
            .filterKeys((r: String) => r == col)
            .filter((r: (String, ColumnData)) =>
              executePredicate(r, columnDefinition.get.dataType, predicate)
            )
          passFilter.nonEmpty
        })
        result
      } else {
        dataFrame
      }
    }

    DataFrameBuilder()
      .withSchema(tableSchema = dataFrameSchema)
      .withData(tableSet)
      .build()
  }
}
