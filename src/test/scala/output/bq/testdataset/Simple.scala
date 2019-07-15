package output.bq.testdataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

case class Simple(foo: String, bar: Long)

object Simple {
  implicit class ToBqRow(val x: Simple) {
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("foo", x.foo)
      put("bar", x.bar)
    }
  }

}
