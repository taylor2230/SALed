package org.saled
package data.common

import org.saled.data.structures.table.TableSchema

trait Options {
  val hasSchema: Option[TableSchema]
}
