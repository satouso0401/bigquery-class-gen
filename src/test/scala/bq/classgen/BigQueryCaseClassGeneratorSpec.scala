package bq.classgen

import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.ZonedDateTime
import java.util

import org.scalatest._
import bq.classgen.TestTables._
import com.google.cloud.bigquery._
import output.bq.testdataset._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class BigQueryCaseClassGeneratorSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfter
    with DiagrammedAssertions {

  val datasetId = "test_dataset"
  val outputDir = "src/test/resources"
  val outputPkg = "output.bq.testdataset"

  val expectFileDir = "src/test/resources/output/bq/testdataset/"
  val actualFileDir = "src/test/scala/output/bq/testdataset/"

  val bigQuery = BigQueryOptions.getDefaultInstance.getService

  before {
    TestTables.createTable(datasetId)
//    BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)
  }

  "class generator" should {
    "create classes from simple table" in {

      val (expectLines, actualLines) = readLinesPair("simple")
      expectLines shouldBe actualLines
    }

    "create classes from structured table" in {

      val (expectLines, actualLines) = readLinesPair(STRUCTURED_TABLE.tableId)
      expectLines shouldBe actualLines
    }

    "create classes from various type table" in {

      val (expectLines, actualLines) = readLinesPair(VARIOUS_TYPE_TABLE.tableId)
      expectLines shouldBe actualLines
    }

    "create classes from nullable and repeated table" in {

      val (expectLines, actualLines) = readLinesPair(NULLABLE_AND_REPEATED_TABLE.tableId)
      expectLines shouldBe actualLines
    }

    "create classes from nullable and repeated struct table" in {

      val (expectLines, actualLines) = readLinesPair(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId)
      expectLines shouldBe actualLines
    }
  }

  "generated class" should {
    "insert to simple table" in {

      val insertId   = java.util.UUID.randomUUID.toString
      val rowContent = Simple(insertId, 1).toBqRow
      insertBq("simple", insertId, rowContent) shouldBe true

      val queryRes = selectBq("simple", "foo", insertId)
      queryRes.get("foo").getStringValue shouldBe insertId
      queryRes.get("bar").getLongValue shouldBe 1

    }

    "insert to structured table" in {
      val insertId = java.util.UUID.randomUUID.toString
      val rowContent =
        Structured(insertId, 1, 2, StructField(3, 4), NestedStruct1(5, NestedStruct2(6))).toBqRow
      insertBq(STRUCTURED_TABLE.tableId, insertId, rowContent) shouldBe true

      val queryRes = selectBq(STRUCTURED_TABLE.tableId, "id", insertId)
      queryRes.get("id").getStringValue shouldBe insertId
      queryRes.get("int_1").getLongValue shouldBe 1
      queryRes.get("int_2").getLongValue shouldBe 2
      queryRes.get("struct_field").getRecordValue.get(0).getLongValue shouldBe 3
      queryRes.get("struct_field").getRecordValue.get(1).getLongValue shouldBe 4
      queryRes.get("nested_struct_1").getRecordValue.get(0).getLongValue shouldBe 5
      queryRes
        .get("nested_struct_1")
        .getRecordValue
        .get(1)
        .getRecordValue
        .get(0)
        .getLongValue shouldBe 6

    }

    "insert to various type table" in {}

    "insert to nullable and repeated table" in {}

    "insert to nullable and repeated struct table" in {}

  }

  def readLinesPair(tableId: String) = {
    val path1       = Paths.get(s"$expectFileDir/${tableId.UCamel + ".scala"}")
    val expectLines = Files.readAllLines(path1, StandardCharsets.UTF_8).asScala

    val path2       = Paths.get(s"$actualFileDir/${tableId.UCamel + ".scala"}")
    val actualLines = Files.readAllLines(path2, StandardCharsets.UTF_8).asScala

    (expectLines, actualLines)
  }

  private def camelCase(str: String) = {
    str.toLowerCase.split("_").map(_.capitalize).mkString("")
  }

  implicit class CamelCaseUtil(val str: String) {
    def lCamel: String = {
      val s = camelCase(str)
      s(0).toString.toLowerCase + s.tail
    }
    def UCamel: String = camelCase(str)
  }

  def insertBq(tableId: String, insertId: String, rowContent: util.Map[String, Any]) = {

    val tid = TableId.of(datasetId, tableId)

    val response: Try[InsertAllResponse] = Try {
      bigQuery.insertAll(
        InsertAllRequest
          .newBuilder(tid)
          .addRow(insertId, rowContent)
          .build)
    }

    response match {
      case Success(v) if v.hasErrors =>
        v.getInsertErrors.asScala.foreach(x => println("# error: " + x))
        false
      case Success(v) => true
      case Failure(e) =>
        e.printStackTrace()
        false
    }
  }

  def selectBq(tableId: String, selectField: String, selectKey: String) = {
    val queryConfig = QueryJobConfiguration
      .newBuilder(s"select * from $datasetId.$tableId where $selectField = '$selectKey'")
      .setUseLegacySql(false)
      .build

    val queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build).waitFor()

    queryJob.getQueryResults().iterateAll().asScala.head
  }
}
