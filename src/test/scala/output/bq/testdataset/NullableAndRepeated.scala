package output.bq.testdataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

case class NullableAndRepeated(
    int64Null: Option[Long],
    numericNull: Option[BigDecimal],
    float64Null: Option[Double],
    boolNull: Option[Boolean],
    stringNull: Option[String],
    bytesNull: Option[Array[Byte]],
    dateNull: Option[LocalDate],
    datetimeNull: Option[LocalDateTime],
    timeNull: Option[LocalTime],
    timestampNull: Option[ZonedDateTime],
    int64List: Seq[Long],
    numericList: Seq[BigDecimal],
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
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("int64_null", x.int64Null.map(y => Long.valueOf(y)).getOrElse(null))
      put("numeric_null", x.numericNull.getOrElse(null))
      put("float64_null", x.float64Null.map(y => Double.valueOf(y)).getOrElse(null))
      put("bool_null", x.boolNull.map(y => Boolean.valueOf(y)).getOrElse(null))
      put("string_null", x.stringNull.getOrElse(null))
      put("bytes_null", x.bytesNull.map(Base64.getEncoder.encodeToString).getOrElse(null))
      put("date_null", x.dateNull.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE)).getOrElse(null))
      put(
        "datetime_null",
        x.datetimeNull.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).getOrElse(null)
      )
      put("time_null", x.timeNull.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME)).getOrElse(null))
      put(
        "timestamp_null",
        x.timestampNull.map(_.toInstant.getEpochSecond).map(y => Long.valueOf(y)).getOrElse(null)
      )
      put("int64_list", x.int64List.map(y => Long.valueOf(y)).asJava)
      put("numeric_list", x.numericList.asJava)
      put("float64_list", x.float64List.map(y => Double.valueOf(y)).asJava)
      put("bool_list", x.boolList.map(y => Boolean.valueOf(y)).asJava)
      put("string_list", x.stringList.asJava)
      put("bytes_list", x.bytesList.map(Base64.getEncoder.encodeToString).asJava)
      put("date_list", x.dateList.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE)).asJava)
      put(
        "datetime_list",
        x.datetimeList.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).asJava
      )
      put("time_list", x.timeList.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME)).asJava)
      put(
        "timestamp_list",
        x.timestampList.map(_.toInstant.getEpochSecond).map(y => Long.valueOf(y)).asJava
      )
    }
  }

}
