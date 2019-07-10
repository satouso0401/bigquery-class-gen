BigQuery Class Gen
==================

Generate code of case class from BigQuery schema.
The generated case class also provides functionality to convert Scala objects to BigQuery row content.

## Dependency
__TODO__

## Usage

This library needs to get the schema from BigQuery. Set the credential that has "bigquery.tables.list" and "bigquery.tables.get" permissions to the environment variable.

```
export GOOGLE_APPLICATION_CREDENTIALS="[CREDENTIAL_JSON]"
```

Generate a case class based on the table contained in the dataset.

```
val datasetId = "test_dataset"
val outputDir = "src/main/scala"
val outputPkg = "output.bq.testdataset"

BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)
```

The generated case class can be converted to a BigQuery row using .toBqRow

```
val bigQuery = BigQueryOptions.getDefaultInstance.getService

val datasetId = "test_dataset"
val tableId   = "play_list"

val rowContent = PlayList(1, "Lotus", 265, "Susumu Hirasawa").toBqRow // convert to BQ row

bigQuery.insertAll(
  InsertAllRequest
    .newBuilder(TableId.of(datasetId, tableId))
    .addRow(rowContent)
    .build)

```

## Sample code
__TODO__
