package org.saled
package data.pipeline.memory

import data.structures.table.{
  ColumnDefinition,
  DataFrame,
  DataFrameBuilder,
  Schema,
  SchemaBuilder,
  SchemaDDL,
  ToDataFrame
}

trait FromMemory {
  def fromMemory(seq: Seq[Seq[?]]): DataFrame = {
    if (seq.nonEmpty) {
      val inferredSchema: Schema = {
        val columns: List[ColumnDefinition] =
          SchemaDDL.inferSchemaDDL(seq.size)
        SchemaBuilder().withSchema(columns).build()
      }
      ToDataFrame.createDataFrame(seq, inferredSchema)
    } else {
      DataFrameBuilder().empty()
    }
  }
}
