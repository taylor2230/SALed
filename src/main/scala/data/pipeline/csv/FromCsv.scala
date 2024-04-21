package org.saled
package data.pipeline.csv

import data.structures.table.*

import java.nio.file.Path
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.io.{BufferedSource, Source}

trait FromCsv {
  private val explodeCsvString: (String, String) => List[List[String]] =
    (delimiter: String, csvContent: String) => {
      val splitRows: List[String] = csvContent.split("\n").toList
      splitRows.par
        .map((r: String) => {
          r.split(delimiter).toList
        })
        .toList
    }

  private val defineCsvSchema: (List[List[String]], CsvOptions) => Schema =
    (csv: List[List[String]], csvOptions: CsvOptions) => {
      if (csvOptions.hasSchema.nonEmpty) {
        csvOptions.hasSchema.get
      } else if (csvOptions.hasHeader.get) {
        val headerDDL =
          csv.head.map((c: String) => { s"$c String" }).mkString(",")
        val csvHeaderDDL: List[ColumnDefinition] =
          SchemaDDL.createSchema(headerDDL)
        SchemaBuilder().withSchema(csvHeaderDDL).build()
      } else {
        val csvInferredSchemaDDL: List[ColumnDefinition] =
          SchemaDDL.inferSchemaDDL(csv.head.size)
        SchemaBuilder().withSchema(csvInferredSchemaDDL).build()
      }
    }

  def fromCsv(csvString: String, csvOptions: CsvOptions): DataFrame = {
    val explodedCsv: List[List[String]] =
      explodeCsvString(csvOptions.hasSeparator.get, csvString)

    val tableSchema: Schema = defineCsvSchema(explodedCsv, csvOptions)

    val csv: List[List[String]] = {
      if (csvOptions.hasHeader.get) {
        explodedCsv.tail
      } else {
        explodedCsv
      }
    }

    ToDataFrame.createDataFrame(csv, tableSchema)
  }

  def fromCsv(csvPath: Path, csvOptions: CsvOptions): DataFrame = {
    val file: BufferedSource = Source.fromFile(csvPath.toUri)
    val fileContents: String = file.getLines().mkString("\n")
    fromCsv(fileContents, csvOptions)
  }
}
