package org.saled
package data.pipeline.iceberg

import data.builder.Builder

import org.apache.iceberg.catalog.TableIdentifier

case class IcebergTableOptions(
    dataset: Option[String] = None,
    tableName: Option[String] = None
) {
  
  def getTableIdentifier: TableIdentifier = {
    TableIdentifier.of(dataset.orNull, tableName.orNull)
  }
}

case class IcebergTableOptionsBuilder(
    dataset: Option[String] = None,
    tableName: Option[String] = None
) extends Builder[IcebergTableOptions] {

  def withDataset(dataset: String): IcebergTableOptionsBuilder = {
    if (dataset.isBlank) {
      copy(dataset = None)
    } else {
      copy(dataset = Some(dataset))
    }
  }

  def withTableName(tableName: String): IcebergTableOptionsBuilder = {
    if (tableName.isBlank) {
      copy(tableName = None)
    } else {
      copy(tableName = Some(tableName))
    }
  }

  override def build(): IcebergTableOptions =
    IcebergTableOptions(dataset = dataset, tableName = tableName)
}
