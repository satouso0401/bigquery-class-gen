import java.util.UUID

import com.google.cloud.bigquery.{StandardTableDefinition, _}

import scala.jdk.CollectionConverters._

object Main extends App {

  //  export GOOGLE_APPLICATION_CREDENTIALS="****"

  val projectId = "****"
  val datasetId = "test_dataset"

  def createTableUsingJson(datasetId: String, tableId: String, schemaJson: String) = {
    val bigquery        = BigQueryOptions.getDefaultInstance.getService
    val dataset         = bigquery.getDataset(datasetId)
    val schema          = BqJsonToBqSchema.convert(schemaJson)
    val tableDefinition = StandardTableDefinition.of(schema)
    dataset.create(tableId, tableDefinition)
  }

  def createTableUsingSql(createQuery: String) = {
    val bigquery = BigQueryOptions.getDefaultInstance.getService

    val queryConfig = QueryJobConfiguration
      .newBuilder(createQuery)
      .setUseLegacySql(false)
      .build()

    val jobId    = JobId.of(UUID.randomUUID.toString)
    var queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)

    queryJob = queryJob.waitFor()

    if (queryJob == null) throw new RuntimeException("Job no longer exists")
    else if (queryJob.getStatus.getError != null) {
      throw new RuntimeException(queryJob.getStatus.getError.toString)
    }

  }

  // 1. データセットの作成

//  val schemaJson = s"""[
//                        {"description": "quarter", "mode": "REQUIRED", "name": "qtr", "type": "STRING"},
//                        {"description": "sales representative", "mode": "NULLABLE", "name": "rep", "type": "STRING"},
//                        {"description": "total sales", "mode": "NULLABLE", "name": "sales", "type": "FLOAT"}
//                      ]"""
//  createTableUsingJson(datasetId, "foo_table_4", schemaJson)
//
//
//  val createQuery = s"create table `$projectId.$datasetId.foo_table_5`(bar string, baz int64)"
//  createTableUsingSql(createQuery)

  // 2. case classの作成

  val bigquery = BigQueryOptions.getDefaultInstance.getService
  val table    = bigquery.getTable(datasetId, "foo_table_4")
  val schema   = table.getDefinition[TableDefinition]().getSchema

  val fieldList = schema.getFields.iterator().asScala

  // ここを再帰にすればmode REPEATEDでもうまくいくかも
  val classFieldList = for (f <- fieldList) yield {

    import com.google.cloud.bigquery.Field.Mode._

    f.getMode match {
      case REPEATED => throw new UnsupportedOperationException("repeatable fields not supported")
      case _ =>
        import com.google.cloud.bigquery.StandardSQLTypeName._

        val name = lcc(f.getName)
        val typeStr = f.getType.getStandardType match {
          case STRING    => "String"
          case INT64     => "Long"
          case FLOAT64   => "Double"
          case TIMESTAMP => "ZonedDateTime"
          case x         => throw new UnsupportedOperationException(s"$x field not supported")
        }
        val nullableWrapType = if (NULLABLE == f.getMode) { s"Option[$typeStr]" } else typeStr

        s"$name: $nullableWrapType"
    }

  }

  println(classFieldList.mkString(", "))

  def lcc(str: String) = {
    val s = camelCase(str)
    s(0).toString.toLowerCase + s.tail
  }

  def ucc(str: String) = camelCase(str)

  def camelCase(str: String) = {
    str.toLowerCase.split("_").map(_.capitalize).mkString("")
  }

}
