package bq.classgen

import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time._
import java.time.temporal.ChronoUnit
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
    BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)
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

    "insert to various type table" in {
      val insertId = java.util.UUID.randomUUID.toString
      val rowContent =
        VariousType(
          1,
          2,
          3.0,
          true,
          insertId,
          Array(1.toByte),
          LocalDate.parse("2007-12-03"),
          LocalDateTime.parse("2007-12-03T10:15:30"),
          LocalTime.parse("10:15:30"),
          ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")
        ).toBqRow
      insertBq(VARIOUS_TYPE_TABLE.tableId, insertId, rowContent) shouldBe true

      val queryRes = selectBq(VARIOUS_TYPE_TABLE.tableId, "string", insertId)
      queryRes.get("int64").getLongValue shouldBe 1
      queryRes.get("numeric").getNumericValue.intValue() shouldBe 2
      queryRes.get("float64").getDoubleValue shouldBe 3.0
      queryRes.get("bool").getBooleanValue shouldBe true
      queryRes.get("string").getStringValue shouldBe insertId
      queryRes.get("bytes").getBytesValue shouldBe Array(1.toByte)
      queryRes.get("date").getStringValue shouldBe "2007-12-03"
      queryRes.get("datetime").getStringValue shouldBe "2007-12-03T10:15:30"
      queryRes.get("time").getStringValue shouldBe "10:15:30"
      queryRes.get("timestamp").getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)

    }

    "insert to nullable and repeated table" in {
      val insertId1 = java.util.UUID.randomUUID.toString
      val rowContent1 =
        NullableAndRepeated(
          Some(1),
          Some(2),
          Some(3.0),
          Some(true),
          Some(insertId1),
          Some(Array(1.toByte)),
          Some(LocalDate.parse("2007-12-03")),
          Some(LocalDateTime.parse("2007-12-03T10:15:30")),
          Some(LocalTime.parse("10:15:30")),
          Some(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")),
          Seq(1, 2),
          Seq(2L, 3L),
          Seq(3.0, 4.0),
          Seq(true, false),
          Seq(insertId1, "str"),
          Seq(Array(1.toByte), Array(2.toByte)),
          Seq(LocalDate.parse("2007-12-03"), LocalDate.parse("2007-12-04")),
          Seq(LocalDateTime.parse("2007-12-03T10:15:30"),
              LocalDateTime.parse("2007-12-03T10:15:31")),
          Seq(LocalTime.parse("10:15:30"), LocalTime.parse("10:15:31")),
          Seq(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"),
              ZonedDateTime.parse("2007-12-03T10:15:31+01:00[Europe/Paris]"))
        ).toBqRow
      insertBq(NULLABLE_AND_REPEATED_TABLE.tableId, insertId1, rowContent1) shouldBe true

      val queryRes1 = selectBq(NULLABLE_AND_REPEATED_TABLE.tableId, "string_null", insertId1)
      queryRes1.get("int64_null").getLongValue shouldBe 1
      queryRes1.get("numeric_null").getNumericValue.intValue() shouldBe 2
      queryRes1.get("float64_null").getDoubleValue shouldBe 3.0
      queryRes1.get("bool_null").getBooleanValue shouldBe true
      queryRes1.get("string_null").getStringValue shouldBe insertId1
      queryRes1.get("bytes_null").getBytesValue shouldBe Array(1.toByte)
      queryRes1.get("date_null").getStringValue shouldBe "2007-12-03"
      queryRes1.get("datetime_null").getStringValue shouldBe "2007-12-03T10:15:30"
      queryRes1.get("time_null").getStringValue shouldBe "10:15:30"
      queryRes1.get("timestamp_null").getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)

      queryRes1.get("int64_list").getRepeatedValue.get(0).getLongValue shouldBe 1
      queryRes1.get("int64_list").getRepeatedValue.get(1).getLongValue shouldBe 2
      queryRes1.get("numeric_list").getRepeatedValue.get(0).getNumericValue.intValue() shouldBe 2
      queryRes1.get("numeric_list").getRepeatedValue.get(1).getNumericValue.intValue() shouldBe 3
      queryRes1.get("float64_list").getRepeatedValue.get(0).getDoubleValue shouldBe 3.0
      queryRes1.get("float64_list").getRepeatedValue.get(1).getDoubleValue shouldBe 4.0
      queryRes1.get("bool_list").getRepeatedValue.get(0).getBooleanValue shouldBe true
      queryRes1.get("bool_list").getRepeatedValue.get(1).getBooleanValue shouldBe false
      queryRes1.get("string_list").getRepeatedValue.get(0).getStringValue shouldBe insertId1
      queryRes1.get("string_list").getRepeatedValue.get(1).getStringValue shouldBe "str"
      queryRes1.get("bytes_list").getRepeatedValue.get(0).getBytesValue shouldBe Array(1.toByte)
      queryRes1.get("bytes_list").getRepeatedValue.get(1).getBytesValue shouldBe Array(2.toByte)
      queryRes1.get("date_list").getRepeatedValue.get(0).getStringValue shouldBe "2007-12-03"
      queryRes1.get("date_list").getRepeatedValue.get(1).getStringValue shouldBe "2007-12-04"
      queryRes1
        .get("datetime_list")
        .getRepeatedValue
        .get(0)
        .getStringValue shouldBe "2007-12-03T10:15:30"
      queryRes1
        .get("datetime_list")
        .getRepeatedValue
        .get(1)
        .getStringValue shouldBe "2007-12-03T10:15:31"
      queryRes1.get("time_list").getRepeatedValue.get(0).getStringValue shouldBe "10:15:30"
      queryRes1.get("time_list").getRepeatedValue.get(1).getStringValue shouldBe "10:15:31"
      queryRes1
        .get("timestamp_list")
        .getRepeatedValue
        .get(0)
        .getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)
      queryRes1
        .get("timestamp_list")
        .getRepeatedValue
        .get(1)
        .getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:31+01:00[Europe/Paris]").toInstant)

      val insertId2 = java.util.UUID.randomUUID.toString
      val rowContent2 =
        NullableAndRepeated(
          None,
          None,
          None,
          None,
          Some(insertId2),
          None,
          None,
          None,
          None,
          None,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil,
          Nil
        ).toBqRow
      insertBq(NULLABLE_AND_REPEATED_TABLE.tableId, insertId2, rowContent2) shouldBe true

      val queryRes2 = selectBq(NULLABLE_AND_REPEATED_TABLE.tableId, "string_null", insertId2)
      queryRes2.get("int64_null").isNull shouldBe true
      queryRes2.get("int64_null").isNull shouldBe true
      queryRes2.get("numeric_null").isNull shouldBe true
      queryRes2.get("float64_null").isNull shouldBe true
      queryRes2.get("bool_null").isNull shouldBe true
      queryRes2.get("string_null").getStringValue shouldBe insertId2
      queryRes2.get("bytes_null").isNull shouldBe true
      queryRes2.get("date_null").isNull shouldBe true
      queryRes2.get("datetime_null").isNull shouldBe true
      queryRes2.get("time_null").isNull shouldBe true
      queryRes2.get("timestamp_null").isNull shouldBe true
      queryRes2.get("int64_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("numeric_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("float64_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("bool_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("string_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("bytes_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("date_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("datetime_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("time_list").getRepeatedValue.isEmpty shouldBe true
      queryRes2.get("timestamp_list").getRepeatedValue.isEmpty shouldBe true

    }

    "insert to nullable and repeated struct table" in {

      val insertId1 = java.util.UUID.randomUUID.toString
      val rowContent1 =
        NullableAndRepeatedStruct(
          insertId1,
          StructFieldRequired(1, ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")),
          Some(
            StructFieldNull(Some(2),
                            Some(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")))),
          Seq(
            StructFieldList(Seq(3),
                            Seq(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"))))
        ).toBqRow
      insertBq(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId, insertId1, rowContent1) shouldBe true

      val queryRes1 = selectBq(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId, "id", insertId1)

      queryRes1.get("id").getStringValue shouldBe insertId1

      queryRes1.get("struct_field_required").getRecordValue.get(0).getLongValue shouldBe 1
      queryRes1
        .get("struct_field_required")
        .getRecordValue
        .get(1)
        .getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)

      queryRes1.get("struct_field_null").getRecordValue.get(0).getLongValue shouldBe 2
      queryRes1
        .get("struct_field_null")
        .getRecordValue
        .get(1)
        .getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)

      queryRes1
        .get("struct_field_list")
        .getRepeatedValue
        .get(0)
        .getRecordValue
        .get(0)
        .getRepeatedValue
        .get(0)
        .getLongValue shouldBe 3
      queryRes1
        .get("struct_field_list")
        .getRepeatedValue
        .get(0)
        .getRecordValue
        .get(1)
        .getRepeatedValue
        .get(0)
        .getTimestampValue shouldBe ChronoUnit.MICROS.between(
        Instant.EPOCH,
        ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]").toInstant)

      val insertId2 = java.util.UUID.randomUUID.toString
      val rowContent2 =
        NullableAndRepeatedStruct(
          insertId2,
          StructFieldRequired(1, ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")),
          None,
          Nil
        ).toBqRow
      insertBq(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId, insertId2, rowContent2) shouldBe true

      val queryRes2 = selectBq(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId, "id", insertId2)

      queryRes2.get("struct_field_null").isNull shouldBe true
      queryRes2.get("struct_field_list").getRepeatedValue.isEmpty shouldBe true

    }

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

  def insertBq(tableId: String, insertId: String, rowContent: util.Map[String, _]) = {

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
      case Success(_) => true
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
