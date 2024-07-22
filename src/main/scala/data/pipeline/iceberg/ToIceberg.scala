package org.saled
package data.pipeline.iceberg

import data.structures.table.DataFrame

trait ToIceberg {
  def mergeTable(
      dataFrame: DataFrame,
      icebergCatalog: IcebergCatalog,
      icebergTableOptions: IcebergTableOptions
  ): Unit = {}

  def replaceTable(
      dataFrame: DataFrame,
      icebergCatalog: IcebergCatalog,
      icebergTableOptions: IcebergTableOptions
  ): Unit = {}

  def appendTable(
      dataFrame: DataFrame,
      icebergCatalog: IcebergCatalog,
      icebergTableOptions: IcebergTableOptions
  ): Unit = {}

  def createTable(
      dataFrame: DataFrame,
      icebergCatalog: IcebergCatalog,
      icebergTableOptions: IcebergTableOptions
  ): Unit = {}
}
