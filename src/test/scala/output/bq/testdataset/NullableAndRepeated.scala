package output.bq.testdataset

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

case class NullableAndRepeated(
    int64Null: Option[Long],
    numericNull: Option[Long],
    float64Null: Option[Double],
    boolNull: Option[Boolean],
    stringNull: Option[String],
    bytesNull: Option[Array[Byte]],
    dateNull: Option[LocalDate],
    datetimeNull: Option[LocalDateTime],
    timeNull: Option[LocalTime],
    timestampNull: Option[ZonedDateTime],
    int64List: Seq[Long],
    numericList: Seq[Long],
    float64List: Seq[Double],
    boolList: Seq[Boolean],
    stringList: Seq[String],
    bytesList: Seq[Array[Byte]],
    dateList: Seq[LocalDate],
    datetimeList: Seq[LocalDateTime],
    timeList: Seq[LocalTime],
    timestampList: Seq[ZonedDateTime]
)

object NullableAndRepeated {
  implicit class ToBqRow(val x: NullableAndRepeated) {
    def toBqRow = {
      Map(
        "int64_null"   -> x.int64Null.getOrElse(null),
        "numeric_null" -> x.numericNull.getOrElse(null),
        "float64_null" -> x.float64Null.getOrElse(null),
        "bool_null"    -> x.boolNull.getOrElse(null),
        "string_null"  -> x.stringNull.getOrElse(null),
        "bytes_null"   -> x.bytesNull.map(y => Base64.getEncoder.encodeToString(y)).getOrElse(null),
        "date_null"    -> x.dateNull.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE)).getOrElse(null),
        "datetime_null" -> x.datetimeNull
          .map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
          .getOrElse(null),
        "time_null"      -> x.timeNull.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME)).getOrElse(null),
        "timestamp_null" -> x.timestampNull.map(_.toInstant.getEpochSecond).getOrElse(null),
        "int64_list"     -> x.int64List.asJava,
        "numeric_list"   -> x.numericList.asJava,
        "float64_list"   -> x.float64List.asJava,
        "bool_list"      -> x.boolList.asJava,
        "string_list"    -> x.stringList.asJava,
        "bytes_list"     -> x.bytesList.map(y => Base64.getEncoder.encodeToString(y)).asJava,
        "date_list"      -> x.dateList.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE)).asJava,
        "datetime_list" -> x.datetimeList
          .map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
          .asJava,
        "time_list"      -> x.timeList.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME)).asJava,
        "timestamp_list" -> x.timestampList.map(_.toInstant.getEpochSecond).asJava
      )
    }.asJava
  }

}
