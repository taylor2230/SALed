package org.saled
package data.pipeline.iceberg

import data.builder.Builder

import org.apache.iceberg.{CatalogProperties, rest}
import org.apache.iceberg.rest.RESTCatalog

case class IcebergCatalog(
    catalogURI: Option[String] = None,
    warehouseLocation: Option[String] = None,
    fileIO: Option[String] = None
) {

  private def getConfiguration: Map[String, String] = {
    Map(
      CatalogProperties.CATALOG_IMPL -> "org.apache.iceberg.rest.RESTCatalog",
      CatalogProperties.URI -> catalogURI.orNull,
      CatalogProperties.WAREHOUSE_LOCATION -> warehouseLocation.orNull,
      CatalogProperties.FILE_IO_IMPL -> fileIO.orNull
    )
  }

  def getCatalog: RESTCatalog = {
    val RESTCatalog: RESTCatalog = rest.RESTCatalog()
    RESTCatalog.setConf(getConfiguration)
    RESTCatalog
  }
}

case class IcebergCatalogBuilder(
    catalogURI: Option[String] = None,
    warehouseLocation: Option[String] = None,
    fileIO: Option[String] = None
) extends Builder[IcebergCatalog] {

  def withCatalogURI(catalogURI: String): IcebergCatalogBuilder = {
    if (catalogURI.isEmpty) {
      copy(catalogURI = None)
    } else {
      copy(catalogURI = Some(catalogURI))
    }
  }

  def withWarehouseLocation(
      warehouseLocation: String
  ): IcebergCatalogBuilder = {
    if (warehouseLocation.isEmpty) {
      copy(warehouseLocation = None)
    } else {
      copy(warehouseLocation = Some(warehouseLocation))
    }
  }

  def withFileIO(fileIO: String): IcebergCatalogBuilder = {
    if (fileIO.isEmpty) {
      copy(fileIO = None)
    } else {
      copy(fileIO = Some(fileIO))
    }
  }

  override def build(): IcebergCatalog = IcebergCatalog(
    catalogURI = catalogURI,
    warehouseLocation = warehouseLocation,
    fileIO = fileIO
  )
}
