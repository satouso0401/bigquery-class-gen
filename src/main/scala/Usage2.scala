import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, InsertAllResponse, TableId}
import com.typesafe.config.ConfigFactory
import output.bq.TestDataset._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
object Usage2 extends App {

  val conf      = ConfigFactory.load()
  val datasetId = conf.getString("bqcodegen.dataset.id")

  val bigQuery = BigQueryOptions.getDefaultInstance.getService

//  val tableId    = TableId.of(datasetId, "sample_table_1")
//  val rowContent = SampleTable1("test", 1).toBqRow.asJava

  val tableId    = TableId.of(datasetId, "sample_table_2")
  val rowContent = SampleTable2(1, 2, StructField(3, 4), NestedStruct1(5, NestedStruct2(6))).toBqRow

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
