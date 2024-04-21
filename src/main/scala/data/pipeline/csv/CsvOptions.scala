package org.saled
package data.pipeline.csv

import data.builder.Builder
import data.common.Options
import data.structures.table.Schema

case class CsvOptions(
    hasHeader: Option[Boolean] = Some(false),
    hasSeparator: Option[String] = Some(","),
    hasSchema: Option[Schema] = None
) extends Options

case class CsvOptionsBuilder(
    hasHeader: Option[Boolean] = Some(true),
    hasSeparator: Option[String] = Some(","),
    hasSchema: Option[Schema] = None
) extends Builder[CsvOptions] {

  def withHeader(hasHeader: Boolean): CsvOptionsBuilder =
    copy(hasHeader = Some(hasHeader))

  def withSeparator(hasSeparator: String): CsvOptionsBuilder = {
    if (hasSeparator.isBlank) {
      copy(hasSeparator = None)
    } else {
      copy(hasSeparator = Some(hasSeparator))
    }
  }

  def withSchema(hasSchema: Schema): CsvOptionsBuilder = {
    if (hasSchema.schema.isEmpty) {
      copy(hasSchema = None)
    } else {
      copy(hasSchema = Some(hasSchema))
    }
  }

  override def build(): CsvOptions =
    CsvOptions(
      hasHeader = hasHeader,
      hasSeparator = hasSeparator,
      hasSchema = hasSchema
    )
}
