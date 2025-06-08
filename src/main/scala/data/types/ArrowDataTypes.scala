package org.saled
package data.types

import org.apache.arrow.vector.types.{DateUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType

object ArrowDataTypes {
  case object String extends ArrowDataType[ArrowType.LargeUtf8] {
    override def toString: String = "String"
    override val definition: String = "String"
    override val arrowDataType: ArrowType.LargeUtf8 = ArrowType.LargeUtf8()
  }
  
  case object Boolean extends ArrowDataType[ArrowType.Bool] {
    override def toString: String = "Boolean"
    override val definition: String = "Boolean"
    override val arrowDataType: ArrowType.Bool = ArrowType.Bool()
  }
  
  case object Integer extends ArrowDataType[ArrowType.Int] {
    override def toString: String = "Integer"
    override val definition: String = "Integer"
    override val arrowDataType: ArrowType.Int = ArrowType.Int(64, false)
  }
  
  case object Decimal extends ArrowDataType[ArrowType.Decimal] {
    override def toString: String = "Decimal"
    override val definition: String = "Decimal"
    override val arrowDataType: ArrowType.Decimal = ArrowType.Decimal(64, 38, 128)
  }
  
  case object BigInt extends ArrowDataType[ArrowType.Decimal] {
    override def toString: String = "BigInt"
    override val definition: String = "BigInt"
    override val arrowDataType: ArrowType.Decimal = ArrowType.Decimal(64, 76, 128)
  }
  
  case object Struct extends ArrowDataType[ArrowType.Struct] {
    override def toString: String = "Struct"
    override val definition: String = "Struct"
    override val arrowDataType: ArrowType.Struct = ArrowType.Struct()
  }
  
  case object List extends ArrowDataType[ArrowType.LargeList] {
    override def toString: String = "List"
    override val definition: String = "List"
    override val arrowDataType: ArrowType.LargeList = ArrowType.LargeList()
  }
  
  case object Date extends ArrowDataType[ArrowType.Date] {
    override def toString: String = "Date"
    override val definition: String = "Date"
    override val arrowDataType: ArrowType.Date = ArrowType.Date(DateUnit.DAY)
  }
  
  case object TimestampMs extends ArrowDataType[ArrowType.Timestamp] {
    override def toString: String = "Timestamp"
    override val definition: String = "Timestamp"
    override val arrowDataType: ArrowType.Timestamp = ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
  }

  case object Timestamp extends ArrowDataType[ArrowType.Timestamp] {
    override def toString: String = "Timestamp"
    override val definition: String = "Timestamp"
    override val arrowDataType: ArrowType.Timestamp = ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
  }
  

  def getDatatype(element: Option[Any]): Option[ArrowDataType[?]] = {
    if (element.nonEmpty) {
      element.get match {
        case x: String => Some(String)
        case x: BigInt => Some(BigInt)
        case x: Float => Some(Decimal)
        case x: Double => Some(Decimal)
        case x: Int => Some(Integer)
        case x: Boolean => Some(Boolean)
        case x: List[?] => Some(List)
        case x: Map[?, ?] => Some(Struct)
        case x: Any => Some(String)
        case null => None
      }
    } else {
      Some(ArrowDataTypes.String)
    }
  }
}

