package org.saled
package data.structures.table

import data.builder.Builder

import scala.collection.parallel.immutable.ParMap

case class Tuple(tuple: ParMap[Column, TupleElement] = ParMap.empty) {
  override def toString: String = {
    val tupleString: String = tuple.map(t => {
      s"""${t._1.columnName}: ${t._2.tupleElement.getOrElse("Null")}"""
    }).mkString(", ")
    tupleString
  }
}

case class TupleBuilder private (
    tuple: ParMap[Column, TupleElement] = ParMap.empty
) extends Builder[Tuple] {

  def withTuple(tuple: ParMap[Column, TupleElement]): TupleBuilder = {
    if (tuple.nonEmpty) {
      copy(tuple = tuple)
    } else {
      copy(ParMap.empty)
    }
  }

  override def build(): Tuple = Tuple(tuple = tuple)
}
