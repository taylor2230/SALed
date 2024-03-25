package org.saled
package data.pipeline

import data.pipeline.csv.{FromCsv, ToCsv}
import data.pipeline.json.{FromJson, ToJson}


object Pipeline {
  case object Source extends FromCsv with FromJson
  case object Sink extends ToCsv with ToJson
}
