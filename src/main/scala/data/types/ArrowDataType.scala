package org.saled
package data.types

trait ArrowDataType[E] {
  val arrowDataType: E
  val definition: String
}
