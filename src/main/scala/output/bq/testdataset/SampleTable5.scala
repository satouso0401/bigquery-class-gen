package output.bq.testdataset
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

case class SampleTable5(
    structFieldRequired: StructFieldRequired,
    structFieldNull: Option[StructFieldNull],
    structFieldList: Seq[StructFieldList]
)
case class StructFieldRequired(required1: Long, required2: ZonedDateTime)
case class StructFieldNull(null1: Option[Long], null2: Option[ZonedDateTime])
case class StructFieldList(list1: Seq[Long], list2: Seq[ZonedDateTime])

object SampleTable5 {
  implicit class ToBqRow(val x: SampleTable5) {
    def toBqRow = {
      Map(
        "struct_field_required" -> structFieldRequired(x.structFieldRequired),
        "struct_field_null"     -> x.structFieldNull.map(structFieldNull).getOrElse(null),
        "struct_field_list"     -> x.structFieldList.map(structFieldList).asJava
      )
    }.asJava
  }
  def structFieldRequired(x: StructFieldRequired) = {
    Map("required_1" -> x.required1, "required_2" -> x.required2.toInstant.getEpochSecond)
  }.asJava
  def structFieldNull(x: StructFieldNull) = {
    Map(
      "null_1" -> x.null1.getOrElse(null),
      "null_2" -> x.null2.map(_.toInstant.getEpochSecond).getOrElse(null)
    )
  }.asJava
  def structFieldList(x: StructFieldList) = {
    Map("list_1" -> x.list1.asJava, "list_2" -> x.list2.map(_.toInstant.getEpochSecond).asJava)
  }.asJava
}
