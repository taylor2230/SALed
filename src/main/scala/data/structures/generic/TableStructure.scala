package org.saled
package data.structures.generic

import data.structures.table.{TableSchema, Tuple}

trait TableStructure {
  val tableSchema: TableSchema
  val table: List[Tuple]
}
