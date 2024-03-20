package org.saled
package data.types

sealed trait DatatypeDefinition {
  val datatypeDefinition: String
}

object DataTypes {
  case object Boolean extends DataType[Boolean] with DatatypeDefinition {
    override def toString: String = "Boolean"
    override val datatypeDefinition: String = "Boolean"

    override def typeCast(element: Option[Any]): Option[Boolean] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String  => x.toBooleanOption
          case x: Boolean => Some(x)
          case _          => None
        }
      } else {
        None
      }
    }
  }

  case object Integer extends DataType[Int] with DatatypeDefinition {
    override def toString: String = "Integer"
    override val datatypeDefinition: String = "Integer"

    override def typeCast(element: Option[Any]): Option[Int] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String => x.toIntOption
          case x: Int    => Some(x)
          case _         => None
        }
      } else {
        None
      }
    }
  }

  case object Double extends DataType[Double] with DatatypeDefinition {
    override def toString: String = "Double"
    override val datatypeDefinition: String = "Double"

    override def typeCast(element: Option[Any]): Option[Double] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String => x.toDoubleOption
          case x: Double => Some(x)
          case x: Int    => Some(x.toDouble)
          case _         => None
        }
      } else {
        None
      }
    }
  }

  case object String extends DataType[String] with DatatypeDefinition {
    override def toString: String = "String"
    override val datatypeDefinition: String = "String"

    override def typeCast(element: Option[Any]): Option[String] = {
      if (element.nonEmpty) {
        element.get match {
          case x: String => Some(x)
          case x         => Some(x.toString)
        }
      } else {
        None
      }
    }
  }
}
