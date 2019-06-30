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

  // 1. set credential
  // export GOOGLE_APPLICATION_CREDENTIALS="credential.json"

  // 2. (Option) create table
//  CreateTable.createTableUsingSql(sampleTable1Sql)
//  CreateTable.createTableUsingJson(datasetId, "sample_table_2", sampleTable2Json)

  // 3. create case class
  BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)

}
