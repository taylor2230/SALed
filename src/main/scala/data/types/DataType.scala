package org.saled
package data.types

trait DataType[E] {
  type columnType = E
  def typeCast(element: Option[Any]): Option[E]
}
