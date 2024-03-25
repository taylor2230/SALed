package org.saled

import data.pipeline.Pipeline

object Main {
  def main(args: Array[String]): Unit = {
    val simpleJsonString: String = {
      """{"game":"bloons", "age": 2, "players":[1,2,3]}"""
    }

    val jsonString: String = {
      """{"col_1":"Loki", "col_2":2, "col_3":[1,2,3], "col_4":{"subcol_4A": "A", "subcol_4B": "B"}}"""
    }

    val f = Pipeline.Source.fromJson(Seq(jsonString))

    println(Pipeline.Sink.toJson(f))
  }
}
