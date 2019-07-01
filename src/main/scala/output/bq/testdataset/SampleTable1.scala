package output.bq.testdataset
import java.util
import scala.jdk.CollectionConverters._

case class SampleTable1(foo: String, bar: Long)

object SampleTable1{
  implicit class ToBqRow(val x: SampleTable1) {
    def toBqRow: util.Map[String, Any] = { Map("foo" -> x.foo, "bar" -> x.bar) }.asJava
  }
  
}

