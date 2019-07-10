package bq.classgen

import java.util.UUID

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery._

object BqUtil {

  private val parser = new JacksonFactory()

  private def dtoTableSchemaToBqSchema(dtoSchema: TableSchema): Schema = {
    val fromPbMethod =
      classOf[Schema].getDeclaredMethods.toIterable
        .find(method => method.getName == "fromPb")
        .get

    fromPbMethod.setAccessible(true)
    fromPbMethod.invoke(null, dtoSchema).asInstanceOf[Schema]
  }

  def convert(jsonSchemaString: String): Schema = {
    val fieldsListDto =
      parser
        .createJsonParser(jsonSchemaString)
        .parseArray(classOf[java.util.ArrayList[TableFieldSchema]], classOf[TableFieldSchema])
        .asInstanceOf[java.util.ArrayList[TableFieldSchema]]

    val schemaDto = new TableSchema()
    schemaDto.setFields(fieldsListDto)

    dtoTableSchemaToBqSchema(schemaDto)
  }

  def createTableUsingJson(datasetId: String, tableId: String, schemaJson: String) = {
    val bigquery        = BigQueryOptions.getDefaultInstance.getService
    val dataset         = bigquery.getDataset(datasetId)
    val schema          = convert(schemaJson)
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
}
