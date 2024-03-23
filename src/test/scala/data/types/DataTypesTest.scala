package data.types

import org.saled.data.types.DataTypes
import org.scalatest.funsuite.AnyFunSuiteLike

class DataTypesTest extends AnyFunSuiteLike {
  test("BooleanValidConversion") {
    val t = Some("true")
    val conv = DataTypes.Boolean.typeCast(t)
    assert(conv.nonEmpty)
    assert(conv.get)
  }

  test("BooleanInvalidConversion") {
    val t: Option[Any] = Some("t")
    val conv = DataTypes.Boolean.typeCast(t)
    assert(conv.isEmpty)
  }

  test("IntegerValidConversion") {
    val i = Some("1")
    val conv = DataTypes.Integer.typeCast(i)
    assert(conv.nonEmpty)
    assert(conv.get == 1)
  }

  test("IntegerInvalidConversion") {
    val i = Some("1.01")
    val conv = DataTypes.Integer.typeCast(i)
    assert(conv.isEmpty)
  }

  test("DoubleValidConversion") {
    val i = Some("1.01")
    val conv = DataTypes.Double.typeCast(i)
    assert(conv.nonEmpty)
    assert(conv.get == 1.01d)
  }

  test("DoubleInvalidConversion") {
    val i = Some("t")
    val conv = DataTypes.Double.typeCast(i)
    assert(conv.isEmpty)
  }

  test("FloatValidConversion") {
    val i = Some("1.01")
    val conv = DataTypes.Float.typeCast(i)
    assert(conv.nonEmpty)
    assert(conv.get == 1.01f)
  }

  test("FloatInvalidConversion") {
    val i = Some("t")
    val conv = DataTypes.Float.typeCast(i)
    assert(conv.isEmpty)
  }

  test("StringValidConversion") {
    val i = Some(1)
    val conv = DataTypes.String.typeCast(i)
    assert(conv.nonEmpty)
    assert(conv.get == "1")
  }
}
