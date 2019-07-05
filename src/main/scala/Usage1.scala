import bq.classgen.BigQueryCaseClassGenerator
import com.typesafe.config.ConfigFactory
object Usage1 extends App {

  val conf      = ConfigFactory.load()
  val datasetId = conf.getString("bqcodegen.dataset.id")
  val outputDir = conf.getString("bqcodegen.output.dir")
  val outputPkg = conf.getString("bqcodegen.output.pkg")

  val sampleTable1Sql = s"create table `$datasetId.sample_table_1`(foo string, bar int64)"

  val sampleTable2Json =
    s"""
        [
          {"name": "int_1", "mode": "REQUIRED", "type": "INT64"},
          {"name": "int_2", "mode": "REQUIRED", "type": "INT64"},
          {"name": "struct_field", "mode": "REQUIRED", "type": "RECORD", "fields": [
            {"name": "struct_child_1", "mode": "REQUIRED", "type": "INT64"},
            {"name": "struct_child_2", "mode": "REQUIRED", "type": "INT64"}
          ]},
          {"name": "nested_struct_1", "mode": "REQUIRED", "type": "RECORD", "fields": [
            {"name": "int_3", "mode": "REQUIRED", "type": "INT64"},
            {"name": "nested_struct_2", "mode": "REQUIRED", "type": "RECORD", "fields": [
              {"name": "int_4", "mode": "REQUIRED", "type": "INT64"}
            ]}
          ]}
        ]
    """

  val sampleTable3Json =
    s"""
        [
          {"name": "int64",     "mode": "REQUIRED", "type": "INT64"},
          {"name": "numeric",   "mode": "REQUIRED", "type": "NUMERIC"},
          {"name": "float64",   "mode": "REQUIRED", "type": "FLOAT64"},
          {"name": "bool",      "mode": "REQUIRED", "type": "BOOL"},
          {"name": "string",    "mode": "REQUIRED", "type": "STRING"},
          {"name": "bytes",     "mode": "REQUIRED", "type": "BYTES"},
          {"name": "date",      "mode": "REQUIRED", "type": "DATE"},
          {"name": "datetime",  "mode": "REQUIRED", "type": "DATETIME"},
          {"name": "time",      "mode": "REQUIRED", "type": "TIME"},
          {"name": "timestamp", "mode": "REQUIRED", "type": "TIMESTAMP"}
        ]
    """

  val sampleTable4Json =
    s"""
        [
          {"name": "int64_null",     "mode": "NULLABLE", "type": "INT64"},
          {"name": "numeric_null",   "mode": "NULLABLE", "type": "NUMERIC"},
          {"name": "float64_null",   "mode": "NULLABLE", "type": "FLOAT64"},
          {"name": "bool_null",      "mode": "NULLABLE", "type": "BOOL"},
          {"name": "string_null",    "mode": "NULLABLE", "type": "STRING"},
          {"name": "bytes_null",     "mode": "NULLABLE", "type": "BYTES"},
          {"name": "date_null",      "mode": "NULLABLE", "type": "DATE"},
          {"name": "datetime_null",  "mode": "NULLABLE", "type": "DATETIME"},
          {"name": "time_null",      "mode": "NULLABLE", "type": "TIME"},
          {"name": "timestamp_null", "mode": "NULLABLE", "type": "TIMESTAMP"},
          {"name": "int64_list",     "mode": "REPEATED", "type": "INT64"},
          {"name": "numeric_list",   "mode": "REPEATED", "type": "NUMERIC"},
          {"name": "float64_list",   "mode": "REPEATED", "type": "FLOAT64"},
          {"name": "bool_list",      "mode": "REPEATED", "type": "BOOL"},
          {"name": "string_list",    "mode": "REPEATED", "type": "STRING"},
          {"name": "bytes_list",     "mode": "REPEATED", "type": "BYTES"},
          {"name": "date_list",      "mode": "REPEATED", "type": "DATE"},
          {"name": "datetime_list",  "mode": "REPEATED", "type": "DATETIME"},
          {"name": "time_list",      "mode": "REPEATED", "type": "TIME"},
          {"name": "timestamp_list", "mode": "REPEATED", "type": "TIMESTAMP"}
        ]
    """

  val sampleTable5Json =
    s"""
        [
          {"name": "struct_field_required", "mode": "REQUIRED", "type": "RECORD", "fields": [
            {"name": "required_1", "mode": "REQUIRED", "type": "INT64"},
            {"name": "required_2", "mode": "REQUIRED", "type": "TIMESTAMP"}
          ]},
          {"name": "struct_field_null", "mode": "NULLABLE", "type": "RECORD", "fields": [
            {"name": "null_1", "mode": "NULLABLE", "type": "INT64"},
            {"name": "null_2", "mode": "NULLABLE", "type": "TIMESTAMP"}
          ]},
          {"name": "struct_field_list", "mode": "REPEATED", "type": "RECORD", "fields": [
            {"name": "list_1", "mode": "REPEATED", "type": "INT64"},
            {"name": "list_2", "mode": "REPEATED", "type": "TIMESTAMP"}
          ]}
        ]
    """

  // 1. set credential
  // export GOOGLE_APPLICATION_CREDENTIALS="credential.json"

  // 2. (Option) create table
//  CreateTable.createTableUsingSql(sampleTable1Sql)
//  CreateTable.createTableUsingJson(datasetId, "sample_table_5", sampleTable5Json)

  // 3. create case class
  BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)

}
