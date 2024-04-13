package org.saled
package data.common

import data.structures.table.Schema

trait Options {
  val hasSchema: Option[Schema]
}
