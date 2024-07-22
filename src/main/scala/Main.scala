package org.saled

import data.pipeline.Pipeline
import data.structures.table.DataFrame

object Main:
  def main(args: Array[String]): Unit = {
    val simpleJsonString: String = {
      """{"game":"bloons", "age": 2, "players":[1,2,3], "col_2": 2}"""
    }

    val jsonString: String = {
      """{"game":"doge", "col_1":"Loki", "col_2":2, "col_3":[1,2,3], "col_4":{"subcol_4A": "A", "subcol_4B": "B"}}"""
    }

    val f: DataFrame =
      Pipeline.Source.fromJson(Seq(simpleJsonString, jsonString))

    f.display()
    
    println(Pipeline.Sink.toJson(f))
  }
