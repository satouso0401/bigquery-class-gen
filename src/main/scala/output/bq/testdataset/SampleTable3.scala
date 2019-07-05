package output.bq.testdataset
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

case class SampleTable3(
    int64: Long,
    numeric: Long,
    float64: Double,
    bool: Boolean,
    string: String,
    bytes: Array[Byte],
    date: LocalDate,
    datetime: LocalDateTime,
    time: LocalTime,
    timestamp: ZonedDateTime
)

object SampleTable3 {
  implicit class ToBqRow(val x: SampleTable3) {
    def toBqRow = {
      Map(
        "int64"     -> x.int64,
        "numeric"   -> x.numeric,
        "float64"   -> x.float64,
        "bool"      -> x.bool,
        "string"    -> x.string,
        "bytes"     -> Base64.getEncoder.encodeToString(x.bytes),
        "date"      -> x.date.format(DateTimeFormatter.ISO_LOCAL_DATE),
        "datetime"  -> x.datetime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        "time"      -> x.time.format(DateTimeFormatter.ISO_LOCAL_TIME),
        "timestamp" -> x.timestamp.toInstant.getEpochSecond
      )
    }.asJava
  }

}
