package org.saled

import data.sources.Source
import data.sources.csv.CsvOptionsBuilder
import data.types.DataTypes

import java.nio.file.{Path, Paths}

object Main {
  def main(args: Array[String]): Unit = {
    val csvString: String =
      "LatD, LatM, LatS, NS, LonD, LonM, LonS, EW, City, State\n41,5,59, N,80,39,0, W, Youngstown, OH\n42,52,48, N,97,23,23, W, Yankton, SD\n46,35,59, N,120,30,36, W, Yakima, WA"

    val schemaDDL: String =
      "LatD Integer, LatM Integer, LatS Integer, NS String, LonD Integer, LonM Integer, LonS Integer, EW String, City String, State String"

    val filePathURI: Path = Paths.get("/Users/shanetaylor/Downloads/cities.csv")

/*    val csvSourceDefinedSchema = Source.fromCsv(
      filePathURI,
      CsvOptionsBuilder(hasSchema = Some(tableSchema)).build()
    )*/

    val csvSourceHeaderSchema = Source.fromCsv(
      filePathURI,
      CsvOptionsBuilder(hasHeader = Some(true)).build()
    )
/*
    val csvSourceInferredSchema = Source.fromCsv(
      filePathURI,
      CsvOptionsBuilder().build()
    )*/

    val test: String => Boolean = (s: String) => {
      s.trim.contains("TX")
    }

    val testExpression: String => Option[Any] = (element: String) => {
      Some(element.toInt + 1)
    }

    csvSourceHeaderSchema.castColumnDataType("LatD", DataTypes.Integer).withNewColumn("LatD", "LatD+1", DataTypes.Integer, testExpression).display()
  }
}