package output.bq.testdataset
import java.util
import scala.jdk.CollectionConverters._

case class SampleTable2(int1: Long, int2: Long, structField: StructField, nestedStruct1: NestedStruct1)
case class StructField(structChild1: Long, structChild2: Long)
case class NestedStruct2(int4: Long)
case class NestedStruct1(int3: Long, nestedStruct2: NestedStruct2)

object SampleTable2{
  implicit class ToBqRow(val x: SampleTable2) {
    def toBqRow: util.Map[String, Any] = { Map("int_1" -> x.int1, "int_2" -> x.int2, "struct_field" -> structField(x.structField), "nested_struct_1" -> nestedStruct1(x.nestedStruct1)) }.asJava
  }
  def structField(x: StructField) = { Map("struct_child_1" -> x.structChild1, "struct_child_2" -> x.structChild2)}.asJava
  def nestedStruct2(x: NestedStruct2) = { Map("int_4" -> x.int4)}.asJava
  def nestedStruct1(x: NestedStruct1) = { Map("int_3" -> x.int3, "nested_struct_2" -> nestedStruct2(x.nestedStruct2))}.asJava
}

