package output.bq.testdataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

case class Structured(
    id: String,
    int1: Long,
    int2: Long,
    structField: StructField,
    nestedStruct1: NestedStruct1
)
case class StructField(structChild1: Long, structChild2: Long)
case class NestedStruct2(int4: Long)
case class NestedStruct1(int3: Long, nestedStruct2: NestedStruct2)

object Structured {
  implicit class ToBqRow(val x: Structured) {
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("id", x.id)
      put("int_1", x.int1)
      put("int_2", x.int2)
      put("struct_field", structField(x.structField))
      put("nested_struct_1", nestedStruct1(x.nestedStruct1))
    }
  }
  def structField(x: StructField): util.Map[String, Object] =
    new util.HashMap[String, Object]() {
      put("struct_child_1", x.structChild1)
      put("struct_child_2", x.structChild2)
    }
  def nestedStruct2(x: NestedStruct2): util.Map[String, Object] =
    new util.HashMap[String, Object]() { put("int_4", x.int4) }
  def nestedStruct1(x: NestedStruct1): util.Map[String, Object] =
    new util.HashMap[String, Object]() {
      put("int_3", x.int3)
      put("nested_struct_2", nestedStruct2(x.nestedStruct2))
    }
}
