package bq.classgen

import org.scalatest.{DiagrammedAssertions, FlatSpec}

class BigQueryCaseClassGeneratorSpec extends FlatSpec with DiagrammedAssertions {

  val datasetId = "test_dataset"
  val outputDir = "src/test/resources"
  val outputPkg = "output.bq.testdataset"

  BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)

}
