package org.saled
package data.pipeline.iceberg

import data.structures.table.{DataFrame, DataFrameBuilder}

import org.apache.iceberg.Table
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.IcebergGenerics.ScanBuilder
import org.apache.iceberg.rest.RESTCatalog

trait FromIceberg {

  def fromIceberg(
      icebergCatalog: IcebergCatalog,
      icebergTableOptions: IcebergTableOptions
  ): DataFrame = {
    val catalog: RESTCatalog = icebergCatalog.getCatalog
    val tableIdentifier = icebergTableOptions.getTableIdentifier
    val table: Table = catalog.loadTable(tableIdentifier)
    val tableData: ScanBuilder = IcebergGenerics.read(table)
    DataFrameBuilder().empty()
  }
}
