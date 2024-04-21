package org.saled
package data.structures.implicits

import data.structures.table.SchemaDDL.inferSchemaDDL
import data.structures.table.{DataFrame, SchemaBuilder, ToDataFrame}

object DataFrameImplicits:
  given toDF: Conversion[Seq[?], DataFrame] = (seq: Seq[?]) => {
    import org.saled.data.implicits.Conversions.seqToSubSeq
    ToDataFrame.createDataFrame(seqToSubSeq(seq), SchemaBuilder().withSchema(inferSchemaDDL(1)).build())
  }
