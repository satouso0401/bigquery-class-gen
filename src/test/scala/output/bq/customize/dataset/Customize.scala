package output.bq.customize.dataset

import java.lang.{Long, Double, Boolean}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util
import java.util.Base64
import scala.collection.JavaConverters._

import java.util.Date

case class Customize(foo: String, bar: Int, baz: Date)

object Customize {
  implicit class ToBqRow(val x: Customize) {
    def toBqRow: util.Map[String, Object] = new util.HashMap[String, Object]() {
      put("foo", x.foo)
      put("bar", x.bar)
      put("baz", x.baz.getTime / 1000)
    }
  }

}
