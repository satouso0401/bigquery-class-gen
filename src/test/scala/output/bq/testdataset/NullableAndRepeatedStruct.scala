package output.bq.testdataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

case class NullableAndRepeatedStruct(
    id: String,
    structFieldRequired: StructFieldRequired,
    structFieldNull: Option[StructFieldNull],
    structFieldList: Seq[StructFieldList]
)
case class StructFieldRequired(required1: Long, required2: ZonedDateTime)
case class StructFieldNull(null1: Option[Long], null2: Option[ZonedDateTime])
case class StructFieldList(list1: Seq[Long], list2: Seq[ZonedDateTime])

object NullableAndRepeatedStruct {
  implicit class ToBqRow(val x: NullableAndRepeatedStruct) {
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("id", x.id)
      put("struct_field_required", structFieldRequired(x.structFieldRequired))
      put("struct_field_null", x.structFieldNull.map(structFieldNull).getOrElse(null))
      put("struct_field_list", x.structFieldList.map(structFieldList).asJava)
    }
  }
  def structFieldRequired(x: StructFieldRequired): util.Map[String, Object] =
    new util.HashMap[String, Object]() {
      put("required_1", x.required1)
      put("required_2", x.required2.toInstant.getEpochSecond)
    }
  def structFieldNull(x: StructFieldNull): util.Map[String, Object] =
    new util.HashMap[String, Object]() {
      put("null_1", x.null1.map(y => Long.valueOf(y)).getOrElse(null))
      put(
        "null_2",
        x.null2.map(_.toInstant.getEpochSecond).map(y => Long.valueOf(y)).getOrElse(null)
      )
    }
  def structFieldList(x: StructFieldList): util.Map[String, Object] =
    new util.HashMap[String, Object]() {
      put("list_1", x.list1.map(y => Long.valueOf(y)).asJava)
      put("list_2", x.list2.map(_.toInstant.getEpochSecond).map(y => Long.valueOf(y)).asJava)
    }
}
