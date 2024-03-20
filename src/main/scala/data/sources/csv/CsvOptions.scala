package org.saled
package data.sources.csv

import data.builder.Builder
import data.common.Options
import org.saled.data.structures.table.TableSchema

case class CsvOptions(
    hasHeader: Option[Boolean] = Some(false),
    hasSeparator: Option[String] = Some(","),
    hasSchema: Option[TableSchema] = None
) extends Options

case class CsvOptionsBuilder private (
    hasHeader: Option[Boolean] = Some(true),
    hasSeparator: Option[String] = Some(","),
    hasSchema: Option[TableSchema] = None
) extends Builder[CsvOptions] {

  def withHeader(hasHeader: Boolean): CsvOptionsBuilder =
    copy(hasHeader = Some(hasHeader))

  def withSeparator(hasSeparator: String): CsvOptionsBuilder =
    copy(hasSeparator = Some(hasSeparator))

  def withSchema(hasSchema: TableSchema): CsvOptionsBuilder =
    copy(hasSchema = Some(hasSchema))

  override def build(): CsvOptions =
    CsvOptions(
      hasHeader = hasHeader,
      hasSeparator = hasSeparator,
      hasSchema = hasSchema
    )
}
