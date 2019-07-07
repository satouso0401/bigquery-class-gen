package output.bq.testdataset
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
import java.util.Base64
import scala.collection.JavaConverters._

object SampleTable1Table {
  case class SampleTable1(foo: String, bar: Long)

  object SampleTable1 {
    implicit class ToBqRow(val x: SampleTable1) {
      def toBqRow = { Map("foo" -> x.foo, "bar" -> x.bar) }.asJava
    }

  }

}
