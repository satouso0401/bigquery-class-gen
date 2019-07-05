import java.time.{LocalDate, LocalDateTime, ZonedDateTime}

import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, InsertAllResponse, TableId}
import com.typesafe.config.ConfigFactory
import output.bq.testdataset._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
object Usage2 extends App {

  val conf      = ConfigFactory.load()
  val datasetId = conf.getString("bqcodegen.dataset.id")

  val bigQuery = BigQueryOptions.getDefaultInstance.getService

//  val tableId    = TableId.of(datasetId, "sample_table_1")
//  val rowContent = SampleTable1("test", 1).toBqRow.asJava

//  val tableId    = TableId.of(datasetId, "sample_table_2")
//  val rowContent = SampleTable2(1, 2, StructField(3, 4), NestedStruct1(5, NestedStruct2(6))).toBqRow

//  val tableId    = TableId.of(datasetId, "sample_table_3")
//  val rowContent = SampleTable3(1, 1, 1.0, true, "str", Array(1.toByte), LocalDate.now(), LocalDateTime.now(), LocalDateTime.now().toLocalTime, ZonedDateTime.now()).toBqRow

//  val tableId    = TableId.of(datasetId, "sample_table_4")
//  val rowContent = SampleTable4(
//    Some(1L), Some(1L), Some(1.0), Some(true), Some("str"), Some(Array(1.toByte)), Some(LocalDate.now()), Some(LocalDateTime.now()), Some(LocalDateTime.now().toLocalTime), Some(ZonedDateTime.now()),
//    Seq(1L, 2L), Seq(1L, 2L), Seq(1.0, 2.0), Seq(true, false), Seq("str", "ing"), Seq(Array(1.toByte),Array(2.toByte)), Seq(LocalDate.now(),LocalDate.now()), Seq(LocalDateTime.now(),LocalDateTime.now()), Seq(LocalDateTime.now().toLocalTime,LocalDateTime.now().toLocalTime), Seq(ZonedDateTime.now(),ZonedDateTime.now()),
//  ).toBqRow

//  val tableId    = TableId.of(datasetId, "sample_table_4")
//  val rowContent = SampleTable4(
//    Some(1L), None, None, None, None, None, None, None, None, None,
//    Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil, Nil,
//  ).toBqRow

//  val tableId    = TableId.of(datasetId, "sample_table_5")
//  val rowContent = SampleTable5(
//    StructFieldRequired(1, ZonedDateTime.now()),
//    Some(StructFieldNull(Some(2L), Some(ZonedDateTime.now()))),
//    StructFieldList(3L :: Nil, ZonedDateTime.now() :: Nil) :: StructFieldList(4L :: Nil, ZonedDateTime.now() :: Nil) :: Nil
//  ).toBqRow

  val tableId    = TableId.of(datasetId, "sample_table_5")
  val rowContent = SampleTable5(
    StructFieldRequired(1, ZonedDateTime.now()),
    None,
    Nil
  ).toBqRow

  val response: Try[InsertAllResponse] = Try {
    bigQuery.insertAll(
      InsertAllRequest
        .newBuilder(tableId)
        .addRow(java.util.UUID.randomUUID.toString, rowContent)
        .build)
  }

  response match {
    case Success(v) if v.hasErrors =>
      v.getInsertErrors.asScala.foreach(x => println("# error: " + x))
      throw new RuntimeException("BigQuery insert error")
    case Success(_) =>
    case Failure(e) =>
      e.printStackTrace()
      throw e
  }

}
