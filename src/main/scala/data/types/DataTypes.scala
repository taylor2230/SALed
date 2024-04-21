package org.saled
package data.types

sealed trait DatatypeDefinition {
  val datatypeDefinition: String
}

object DataTypes {
  private def convertListType(
      list: List[?],
      dataType: DataType[?]
  ): Option[List[?]] = {
    try {
      if (list.nonEmpty) {
        val castedList: List[?] = list
          .map((element: Any) => {
            dataType.typeCast(Some(element))
          })
          .filter((element: Option[?]) => element.nonEmpty)
          .map((element: Option[?]) => element.get)

        Some(castedList)
      } else {
        None
      }
    } catch {
      case _: Throwable => println("cast of list elements failed"); None
    }
  }

  private def convertMapType(
      map: Map[?, ?],
      dataType: DataType[?]
  ): Option[Map[String, ?]] = {
    try {
      if (map.nonEmpty) {
        val castedMap: Map[?, ?] = map
          .map((element: (?, ?)) => {
            (element._1, dataType.typeCast(Some(element._2)))
          })
          .map((element: (?, Option[?])) => (element._1, element._2.get))

        Some(castedMap.asInstanceOf[Map[String, ?]])
      } else {
        None
      }
    } catch {
      case _: Throwable => println("cast of list elements failed"); None
    }
  }

  case object Any extends DataType[Any] with DatatypeDefinition {
    override def toString: String = "Any"
    override val datatypeDefinition: String = "Any"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element
      } else {
        None
      }
    }
  }

  case object Boolean extends DataType[Boolean] with DatatypeDefinition {
    override def toString: String = "Boolean"
    override val datatypeDefinition: String = "Boolean"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => x.toBooleanOption
          case x: Boolean   => Some(x)
          case x: List[?]   => convertListType(x, Boolean)
          case x: Map[?, ?] => convertMapType(x, Boolean)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  case object Integer extends DataType[Int] with DatatypeDefinition {
    override def toString: String = "Integer"
    override val datatypeDefinition: String = "Integer"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => x.toIntOption
          case x: BigInt    => Some(x.toInt)
          case x: Float     => Some(x.toInt)
          case x: Double    => Some(x.toInt)
          case x: Int       => Some(x)
          case x: List[?]   => convertListType(x, Integer)
          case x: Map[?, ?] => convertMapType(x, Integer)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  case object Double extends DataType[Double] with DatatypeDefinition {
    override def toString: String = "Double"
    override val datatypeDefinition: String = "Double"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => x.toDoubleOption
          case x: BigInt    => Some(x.toDouble)
          case x: Float     => Some(x.toDouble)
          case x: Double    => Some(x)
          case x: Int       => Some(x.toDouble)
          case x: List[?]   => convertListType(x, Double)
          case x: Map[?, ?] => convertMapType(x, Double)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  case object Float extends DataType[Float] with DatatypeDefinition {
    override def toString: String = "Double"
    override val datatypeDefinition: String = "Double"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => x.toFloatOption
          case x: BigInt    => Some(x.toFloat)
          case x: Float     => Some(x)
          case x: Double    => Some(x.toFloat)
          case x: Int       => Some(x.toFloat)
          case x: List[?]   => convertListType(x, Float)
          case x: Map[?, ?] => convertMapType(x, Float)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  case object BigInteger extends DataType[BigInt] with DatatypeDefinition {
    override def toString: String = "BigInteger"
    override val datatypeDefinition: String = "BigInteger"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => Some(BigInt(x))
          case x: BigInt    => Some(x)
          case x: List[?]   => convertListType(x, BigInteger)
          case x: Map[?, ?] => convertMapType(x, BigInteger)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  case object String extends DataType[String] with DatatypeDefinition {
    override def toString: String = "String"
    override val datatypeDefinition: String = "String"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String    => Some(x)
          case x: BigInt    => Some(x.toString())
          case x: List[?]   => convertListType(x, String)
          case x: Map[?, ?] => convertMapType(x, String)
          case x            => Some(x.toString)
        }
      } else {
        None
      }
    }
  }

  case object List extends DataType[Any] with DatatypeDefinition {
    override def toString: String = "List"
    override val datatypeDefinition: String = "List"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: List[?] => Some(x)
          case _          => None
        }
      } else {
        None
      }
    }
  }

  case object Map extends DataType[Any] with DatatypeDefinition {
    override def toString: String = "Map"
    override val datatypeDefinition: String = "Map"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: Map[?, ?] => Some(x)
          case _            => None
        }
      } else {
        None
      }
    }
  }

  def getDatatype(element: Option[Any]): Option[DataType[?]] = {
    if (element.nonEmpty) {
      element.get match {
        case x: String    => Some(String)
        case x: BigInt    => Some(BigInteger)
        case x: Float     => Some(Float)
        case x: Double    => Some(Double)
        case x: Int       => Some(Integer)
        case x: Boolean   => Some(Boolean)
        case x: List[?]   => Some(List)
        case x: Map[?, ?] => Some(Map)
        case x: Any       => Some(Any)
        case _            => None
      }
    } else {
      Some(DataTypes.String)
    }
  }
}
