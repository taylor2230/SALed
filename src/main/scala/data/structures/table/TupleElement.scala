package org.saled
package data.structures.table

import data.builder.Builder

case class TupleElement(tupleElement: Option[Any] = None) {
  override def toString: String = {
    s"""${tupleElement.getOrElse("Null")}"""
  }
}

case class TupleElementBuilder private (tupleElement: Option[Any] = None)
    extends Builder[TupleElement] {

  def withTupleElement(tupleElement: Option[Any]): TupleElementBuilder =
    copy(tupleElement = tupleElement)

  override def build(): TupleElement = TupleElement(tupleElement = tupleElement)
}
