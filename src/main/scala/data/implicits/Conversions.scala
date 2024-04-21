package org.saled
package data.implicits

object Conversions:
  given seqToSet: Conversion[Seq[String], Set[String]] = (seq: Seq[String]) => seq.toSet
  given seqToSubSeq: Conversion[Seq[?], Seq[Seq[?]]] = (seq: Seq[?]) => seq.map((i: Any) => List(i))
