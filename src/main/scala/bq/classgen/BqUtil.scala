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

  private def convert(jsonSchemaString: String): Schema = {
    val fieldsListDto =
      parser
        .createJsonParser(jsonSchemaString)
        .parseArray(classOf[java.util.ArrayList[TableFieldSchema]], classOf[TableFieldSchema])
        .asInstanceOf[java.util.ArrayList[TableFieldSchema]]

    val schemaDto = new TableSchema()
    schemaDto.setFields(fieldsListDto)

    dtoTableSchemaToBqSchema(schemaDto)
  }

  /**
    * create a table using json
    * @param datasetId dataset id
    * @param tableId id of the table to create
    * @param schemaJson table schema definition
    * @param bigquery BigQuery service object
    */
  def createTableUsingJson(datasetId: String, tableId: String, schemaJson: String)(implicit bigquery: BigQuery): Unit = {
    val bigquery        = BigQueryOptions.getDefaultInstance.getService
    val dataset         = bigquery.getDataset(datasetId)
    val schema          = convert(schemaJson)
    val tableDefinition = StandardTableDefinition.of(schema)
    dataset.create(tableId, tableDefinition)
  }

  /**
    * Create a table using sql
    * @param createQuery table schema definition
    * @param bigquery BigQuery service object
    */
  def createTableUsingSql(createQuery: String)(implicit bigquery: BigQuery) = {

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
