import java.util.UUID

import com.google.cloud.bigquery._
object CreateTable {
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
}
