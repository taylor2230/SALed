package org.saled
package data.structures.generic

import data.structures.table.{Schema, Row}

trait DatasetStructure {
  val dataFrameSchema: Schema
  val dataFrame: List[Row]

  def listToSet(columns: Seq[String]): Set[String] = {
    columns.toSet
  }
}
