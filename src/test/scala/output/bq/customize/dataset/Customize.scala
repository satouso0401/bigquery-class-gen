package output.bq.customize.dataset

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

import java.util.Date

case class Customize(foo: String, bar: Int, baz: Date)

object Customize {
  implicit class ToBqRow(val x: Customize) {
    def toBqRow = { Map("foo" -> x.foo, "bar" -> x.bar, "baz" -> x.baz.getTime / 1000) }.asJava
  }

}
