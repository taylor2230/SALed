package org.saled
package data.structures.table

import data.builder.Builder

import scala.collection.parallel.immutable.ParMap

case class Tuple(tuple: ParMap[Column, TupleElement] = ParMap.empty) {}

case class TupleBuilder private (
    tuple: ParMap[Column, TupleElement] = ParMap.empty
) extends Builder[Tuple] {

  def withTuple(tuple: ParMap[Column, TupleElement]): TupleBuilder =
    copy(tuple = tuple)

  override def build(): Tuple = Tuple(tuple = tuple)
}
