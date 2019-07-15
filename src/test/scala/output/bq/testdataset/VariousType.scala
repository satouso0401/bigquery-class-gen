package output.bq.testdataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

case class VariousType(
    int64: Long,
    numeric: BigDecimal,
    float64: Double,
    bool: Boolean,
    string: String,
    bytes: Array[Byte],
    date: LocalDate,
    datetime: LocalDateTime,
    time: LocalTime,
    timestamp: ZonedDateTime
)

object VariousType {
  implicit class ToBqRow(val x: VariousType) {
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("int64", x.int64)
      put("numeric", x.numeric)
      put("float64", x.float64)
      put("bool", x.bool)
      put("string", x.string)
      put("bytes", Base64.getEncoder.encodeToString(x.bytes))
      put("date", x.date.format(DateTimeFormatter.ISO_LOCAL_DATE))
      put("datetime", x.datetime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      put("time", x.time.format(DateTimeFormatter.ISO_LOCAL_TIME))
      put("timestamp", x.timestamp.toInstant.getEpochSecond)
    }
  }

}
