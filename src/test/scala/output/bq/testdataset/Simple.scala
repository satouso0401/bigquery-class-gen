package output.bq.testdataset

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

case class Simple(foo: String, bar: Long)

object Simple {
  implicit class ToBqRow(val x: Simple) {
    def toBqRow = { Map("foo" -> x.foo, "bar" -> x.bar) }.asJava
  }

}
