package bq.classgen

import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Table}

object TestTables {

  case class TableInfo(tableId: String, schemaJson: String)

  def createTable(datasetId: String)(implicit bigquery: BigQuery) = {
    val dataset  = bigquery.getDataset(datasetId)

    if (Option(dataset.get("simple")).isEmpty) {
      val sql = s"create table `$datasetId.simple`(foo string, bar int64)"
      BqUtil.createTableUsingSql(sql)
    }

    if (Option(dataset.get(STRUCTURED_TABLE.tableId)).isEmpty)
      BqUtil.createTableUsingJson(datasetId,
                                       STRUCTURED_TABLE.tableId,
                                       STRUCTURED_TABLE.schemaJson)

    if (Option(dataset.get(VARIOUS_TYPE_TABLE.tableId)).isEmpty)
      BqUtil.createTableUsingJson(datasetId,
                                       VARIOUS_TYPE_TABLE.tableId,
                                       VARIOUS_TYPE_TABLE.schemaJson)

    if (Option(dataset.get(NULLABLE_AND_REPEATED_TABLE.tableId)).isEmpty)
      BqUtil.createTableUsingJson(datasetId,
                                       NULLABLE_AND_REPEATED_TABLE.tableId,
                                       NULLABLE_AND_REPEATED_TABLE.schemaJson)

    if (Option(dataset.get(NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId)).isEmpty)
      BqUtil.createTableUsingJson(datasetId,
                                       NULLABLE_AND_REPEATED_STRUCT_TABLE.tableId,
                                       NULLABLE_AND_REPEATED_STRUCT_TABLE.schemaJson)

  }

  def createSimpleTable(datasetId: String)(implicit bigquery: BigQuery): Unit = {
    val sql = s"create table `$datasetId.simple`(foo string, bar int64)"
    BqUtil.createTableUsingSql(sql)
  }

  val STRUCTURED_TABLE = TableInfo(
    "structured",
    """[
      |  {"name": "id", "mode": "REQUIRED", "type": "STRING"},
      |  {"name": "int_1", "mode": "REQUIRED", "type": "INT64"},
      |  {"name": "int_2", "mode": "REQUIRED", "type": "INT64"},
      |  {"name": "struct_field", "mode": "REQUIRED", "type": "RECORD", "fields": [
      |    {"name": "struct_child_1", "mode": "REQUIRED", "type": "INT64"},
      |    {"name": "struct_child_2", "mode": "REQUIRED", "type": "INT64"}
      |  ]},
      |  {"name": "nested_struct_1", "mode": "REQUIRED", "type": "RECORD", "fields": [
      |    {"name": "int_3", "mode": "REQUIRED", "type": "INT64"},
      |    {"name": "nested_struct_2", "mode": "REQUIRED", "type": "RECORD", "fields": [
      |      {"name": "int_4", "mode": "REQUIRED", "type": "INT64"}
      |    ]}
      |  ]}
      |]
    """.stripMargin
  )

  val VARIOUS_TYPE_TABLE = TableInfo(
    "various_type",
    """[
      |  {"name": "int64",     "mode": "REQUIRED", "type": "INT64"},
      |  {"name": "numeric",   "mode": "REQUIRED", "type": "NUMERIC"},
      |  {"name": "float64",   "mode": "REQUIRED", "type": "FLOAT64"},
      |  {"name": "bool",      "mode": "REQUIRED", "type": "BOOL"},
      |  {"name": "string",    "mode": "REQUIRED", "type": "STRING"},
      |  {"name": "bytes",     "mode": "REQUIRED", "type": "BYTES"},
      |  {"name": "date",      "mode": "REQUIRED", "type": "DATE"},
      |  {"name": "datetime",  "mode": "REQUIRED", "type": "DATETIME"},
      |  {"name": "time",      "mode": "REQUIRED", "type": "TIME"},
      |  {"name": "timestamp", "mode": "REQUIRED", "type": "TIMESTAMP"}
      |]
    """.stripMargin
  )

  val NULLABLE_AND_REPEATED_TABLE = TableInfo(
    "nullable_and_repeated",
    """[
      |  {"name": "int64_null",     "mode": "NULLABLE", "type": "INT64"},
      |  {"name": "numeric_null",   "mode": "NULLABLE", "type": "NUMERIC"},
      |  {"name": "float64_null",   "mode": "NULLABLE", "type": "FLOAT64"},
      |  {"name": "bool_null",      "mode": "NULLABLE", "type": "BOOL"},
      |  {"name": "string_null",    "mode": "NULLABLE", "type": "STRING"},
      |  {"name": "bytes_null",     "mode": "NULLABLE", "type": "BYTES"},
      |  {"name": "date_null",      "mode": "NULLABLE", "type": "DATE"},
      |  {"name": "datetime_null",  "mode": "NULLABLE", "type": "DATETIME"},
      |  {"name": "time_null",      "mode": "NULLABLE", "type": "TIME"},
      |  {"name": "timestamp_null", "mode": "NULLABLE", "type": "TIMESTAMP"},
      |  {"name": "int64_list",     "mode": "REPEATED", "type": "INT64"},
      |  {"name": "numeric_list",   "mode": "REPEATED", "type": "NUMERIC"},
      |  {"name": "float64_list",   "mode": "REPEATED", "type": "FLOAT64"},
      |  {"name": "bool_list",      "mode": "REPEATED", "type": "BOOL"},
      |  {"name": "string_list",    "mode": "REPEATED", "type": "STRING"},
      |  {"name": "bytes_list",     "mode": "REPEATED", "type": "BYTES"},
      |  {"name": "date_list",      "mode": "REPEATED", "type": "DATE"},
      |  {"name": "datetime_list",  "mode": "REPEATED", "type": "DATETIME"},
      |  {"name": "time_list",      "mode": "REPEATED", "type": "TIME"},
      |  {"name": "timestamp_list", "mode": "REPEATED", "type": "TIMESTAMP"}
      |]
    """.stripMargin
  )

  val NULLABLE_AND_REPEATED_STRUCT_TABLE = TableInfo(
    "nullable_and_repeated_struct",
    """[
      |  {"name": "id", "mode": "REQUIRED", "type": "STRING"},
      |  {"name": "struct_field_required", "mode": "REQUIRED", "type": "RECORD", "fields": [
      |    {"name": "required_1", "mode": "REQUIRED", "type": "INT64"},
      |    {"name": "required_2", "mode": "REQUIRED", "type": "TIMESTAMP"}
      |  ]},
      |  {"name": "struct_field_null", "mode": "NULLABLE", "type": "RECORD", "fields": [
      |    {"name": "null_1", "mode": "NULLABLE", "type": "INT64"},
      |    {"name": "null_2", "mode": "NULLABLE", "type": "TIMESTAMP"}
      |  ]},
      |  {"name": "struct_field_list", "mode": "REPEATED", "type": "RECORD", "fields": [
      |    {"name": "list_1", "mode": "REPEATED", "type": "INT64"},
      |    {"name": "list_2", "mode": "REPEATED", "type": "TIMESTAMP"}
      |  ]}
      |]
    """.stripMargin
  )

}
