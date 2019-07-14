package bq.classgen

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util

import com.google.cloud.bigquery._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object TestUtil {

  def readLinesPair(expectFileDir: String, actualFileDir: String, tableId: String) = {
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

  def insertBq(datasetId: String,
               tableId: String,
               insertId: String,
               rowContent: util.Map[String, _])(implicit bigQuery: BigQuery) = {

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

  def selectBq(datasetId: String, tableId: String, selectField: String, selectKey: String)(
      implicit bigQuery: BigQuery) = {
    val queryConfig = QueryJobConfiguration
      .newBuilder(s"select * from $datasetId.$tableId where $selectField = '$selectKey'")
      .setUseLegacySql(false)
      .build

    val queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).build).waitFor()

    queryJob.getQueryResults().iterateAll().asScala.head
  }

}
