package org.saled
package data.common

import data.structures.table.TableSchema

trait Options {
  val hasSchema: Option[TableSchema]
}
