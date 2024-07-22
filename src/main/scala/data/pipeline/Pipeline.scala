package org.saled
package data.pipeline

import data.pipeline.csv.{FromCsv, ToCsv}
import data.pipeline.iceberg.{FromIceberg, ToIceberg}
import data.pipeline.json.{FromJson, ToJson}
import data.pipeline.memory.FromMemory

object Pipeline:
  case object Source extends FromCsv with FromJson with FromMemory with FromIceberg
  case object Sink extends ToCsv with ToJson with ToIceberg
