package bq.classgen

import java.text.SimpleDateFormat

import bq.classgen.TestUtil._
import com.google.cloud.bigquery.StandardSQLTypeName._
import com.google.cloud.bigquery.{BigQueryOptions, StandardSQLTypeName}
import org.scalatest._
import output.bq.customize.dataset.Customize

class CustomizeClassFieldSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfter
    with DiagrammedAssertions {

  val datasetId = "test_customize_field_dataset"
  val outputDir = "src/test/resources"
  val outputPkg = "output.bq.customize.dataset"

  val expectFileDir = "src/test/resources/output/bq/customize/dataset/"
  val actualFileDir = "src/test/scala/output/bq/customize/dataset/"

  implicit val bigQuery = BigQueryOptions.getDefaultInstance.getService

  before {
    val dataset = bigQuery.getDataset(datasetId)

    if (Option(dataset.get("customize")).isEmpty) {
      val sql = s"create table `$datasetId.customize`(foo string, bar int64, baz timestamp)"
      BqUtil.createTableUsingSql(sql)
    }

    val importClass = "java.util.Date" :: Nil

    val overwriteType: PartialFunction[StandardSQLTypeName, String] = {
      case INT64     => "Int"
      case TIMESTAMP => "Date"
    }

    val overwriteClassToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
      case (INT64, fieldName)     => s"x.$fieldName"
      case (TIMESTAMP, fieldName) => s"x.$fieldName.getTime / 1000"
    }

    BigQueryCaseClassGenerator(
      importClass,
      overwriteType,
      overwriteClassToRow,
      BigQueryCaseClassGenerator.NothingOverwriteFunctorToRow
    ).run(datasetId, outputDir, outputPkg)
  }

  "class generator" should {
    "can customize the field" in {

      val (expectLines, actualLines) = readLinesPair(expectFileDir, actualFileDir, "customize")
      expectLines shouldBe actualLines
    }
  }

  "generated class" should {
    "can be insert into customize tables" in {

      val sdf  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val date = sdf.parse("2019-07-14T23:00:01")

      val insertId   = java.util.UUID.randomUUID.toString
      val rowContent = Customize(insertId, 1, date).toBqRow
      insertBq(datasetId, "customize", insertId, rowContent) shouldBe true

      val queryRes = selectBq(datasetId, "customize", "foo", insertId)
      queryRes.get("foo").getStringValue shouldBe insertId
      queryRes.get("bar").getLongValue shouldBe 1
      queryRes.get("baz").getTimestampValue shouldBe date.getTime * 1000

    }
  }
}
