package org.saled
package data.pipeline

import data.pipeline.csv.{FromCsv, ToCsv}


object Source extends FromCsv {

}

object Sink extends ToCsv {

}

object Pipeline {
  def source: Source.type = {
    Source
  }

  def sink: Sink.type  = {
    Sink
  }
}
