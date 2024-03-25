package org.saled
package data.types

sealed trait DatatypeDefinition {
  val datatypeDefinition: String
}

object DataTypes {
  private def convertListType(
      list: List[_],
      dataType: DataType[_]
  ): Option[List[_]] = {
    try {
      if (list.nonEmpty) {
        val castedList: List[_] = list
          .map((element: Any) => {
            dataType.typeCast(Some(element))
          })
          .filter((element: Option[_]) => element.nonEmpty)
          .map((element: Option[_]) => element.get)

        Some(castedList)
      } else {
        None
      }
    } catch {
      case _: Throwable => println("cast of list elements failed"); None
    }
  }

  private def convertMapType(
      map: Map[String, _],
      dataType: DataType[_]
  ): Option[Map[String, _]] = {
    try {
      if (map.nonEmpty) {
        val castedMap: Map[String, _] = map
          .map((element: (String, Any)) => {
            (element._1, dataType.typeCast(Some(element._2)))
          })
          .map((element: (String, Option[Any])) => (element._1, element._2.get))

        Some(castedMap)
      } else {
        None
      }
    } catch {
      case _: Throwable => println("cast of list elements failed"); None
    }
  }

  case object Boolean extends DataType[Boolean] with DatatypeDefinition {
    override def toString: String = "Boolean"
    override val datatypeDefinition: String = "Boolean"

    override def typeCast(element: Option[Any]): Option[Any] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String         => x.toBooleanOption
          case x: Boolean        => Some(x)
          case x: List[_]        => convertListType(x, Boolean)
          case x: Map[String, _] => convertMapType(x, Boolean)
          case _                 => None
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
          case x: String         => x.toIntOption
          case x: BigInt         => Some(x.toInt)
          case x: Float          => Some(x.toInt)
          case x: Double         => Some(x.toInt)
          case x: Int            => Some(x)
          case x: List[_]        => convertListType(x, Integer)
          case x: Map[String, _] => convertMapType(x, Integer)
          case _                 => None
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
          case x: String         => x.toDoubleOption
          case x: BigInt         => Some(x.toDouble)
          case x: Float          => Some(x.toDouble)
          case x: Double         => Some(x)
          case x: Int            => Some(x.toDouble)
          case x: List[_]        => convertListType(x, Double)
          case x: Map[String, _] => convertMapType(x, Double)
          case _                 => None
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
          case x: String         => x.toFloatOption
          case x: BigInt         => Some(x.toFloat)
          case x: Float          => Some(x)
          case x: Double         => Some(x.toFloat)
          case x: Int            => Some(x.toFloat)
          case x: List[_]        => convertListType(x, Float)
          case x: Map[String, _] => convertMapType(x, Float)
          case _                 => None
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
          case x: String         => Some(BigInt(x))
          case x: BigInt         => Some(x)
          case x: List[_]        => convertListType(x, BigInteger)
          case x: Map[String, _] => convertMapType(x, BigInteger)
          case _                 => None
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
          case x: String         => Some(x)
          case x: BigInt         => Some(x.toString())
          case x: List[_]        => convertListType(x, String)
          case x: Map[String, _] => convertMapType(x, String)
          case x                 => Some(x.toString)
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
          case x: List[_] => Some(x.asInstanceOf[List[Any]])
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
          case x: Map[String, Any] => Some(x)
          case _                   => None
        }
      } else {
        None
      }
    }
  }

  def getDatatype(element: Option[Any]): Option[DataType[_]] = {
    if (element.nonEmpty) {
      element.get match {
        case x: String           => Some(String)
        case x: BigInt           => Some(BigInteger)
        case x: Float            => Some(Float)
        case x: Double           => Some(Double)
        case x: Int              => Some(Integer)
        case x: Boolean          => Some(Boolean)
        case x: List[_]          => Some(List)
        case x: Map[String, Any] => Some(Map)
        case _                   => None
      }
    } else {
      Some(DataTypes.String)
    }
  }
}
