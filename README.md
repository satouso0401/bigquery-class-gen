BigQuery Class Gen
==================

Generate code of case class from BigQuery schema.
The generated case class also provides functionality to convert Scala objects to BigQuery row content.

## Installation

Add to build.sbt

```
libraryDependencies += "com.github.satouso0401" %% "bigquery-class-gen" % "0.2.1"
```

## Usage

This library needs to get the schema from BigQuery. Set the credential that has "bigquery.tables.list" and "bigquery.tables.get" permissions to the environment variable.

```
export GOOGLE_APPLICATION_CREDENTIALS="[CREDENTIAL_JSON]"
```

Generate a case class based on the table contained in the dataset.

```
implicit val bigQuery = BigQueryOptions.getDefaultInstance.getService

val datasetId = "test_dataset"
val outputDir = "src/main/scala"
val outputPkg = "output.bq.testdataset"

BigQueryCaseClassGenerator().run(datasetId, outputDir, outputPkg)
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

## Customize mapping method

Usually, BigQuery TIMESTAMP is mapped to ZonedDateTime. It can be overwritten by giving BigQueryCaseClassGenerator a Partial Function for mapping.

```
val importClass = "java.util.Date" :: Nil

val overwriteType: PartialFunction[StandardSQLTypeName, String] = {
  case INT64     => "Int"
  case TIMESTAMP => "Date"
}

val overwriteClassToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
  case (INT64, fieldName)     => s"x.$fieldName"
  case (TIMESTAMP, fieldName) => s"x.$fieldName.getTime / 1000"
}

BigQueryCaseClassGenerator(
  importClass,
  overwriteType,
  overwriteClassToRow,
  BigQueryCaseClassGenerator.NothingOverwriteFunctorToRow
).run(datasetId, outputDir, outputPkg)
```

## Sample code

https://github.com/satouso0401/bigquery-class-gen-sample
