package org.saled
package data.pipeline.csv

import org.scalatest.funsuite.AnyFunSuiteLike

class CsvOptionsTest extends AnyFunSuiteLike {
  test("CsvOptionsDefaultBuilder") {
    val opts = CsvOptionsBuilder().build()
    assert(opts.hasSchema.isEmpty)
    assert(opts.hasHeader.get)
    assert(opts.hasSeparator.get == ",")
  }

}
