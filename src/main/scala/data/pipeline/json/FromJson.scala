package org.saled
package data.pipeline.json

import data.structures.table._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.parallel.CollectionConverters.ImmutableSeqIsParallelizable
import scala.collection.parallel.immutable.ParSeq

trait FromJson {
  private val extractJson: Seq[String] => ParSeq[Map[String, Any]] =
    (json: Seq[String]) => {
      implicit val formats: DefaultFormats.type = DefaultFormats
      json.par
        .map((js: String) => {
          val parsedJson = parse(js).toOption
          if (parsedJson.nonEmpty) {
            Some(parsedJson.get.extract[Map[String, Any]])
          } else {
            None
          }
        })
        .filter((r: Option[Map[String, Any]]) => { r.nonEmpty })
        .map((r: Option[Map[String, Any]]) => { r.get })
    }

  def fromJson(json: Seq[String]): DataFrame = {
    val extractedJson: ParSeq[Map[String, Any]] = extractJson(json)

    val inferredJsonSchema: Schema = {
      val columns: List[ColumnDefinition] = SchemaDDL.inferJsonSchemaDDL(extractedJson).distinct
      SchemaBuilder().withSchema(columns).build()
    }

    val structuredJsonData: List[List[_]] = extractedJson
      .map((r: Map[String, Any]) => {
        inferredJsonSchema.schema.map((c: ColumnDefinition) => {
          r.getOrElse(c.columnName, None)
        })
      })
      .toList

    val tableSet: DataFrame =
      ToDataFrame.createDataFrame(structuredJsonData, inferredJsonSchema)

    tableSet
  }
}
