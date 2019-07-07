package bq.classgen

import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest._

import scala.collection.JavaConverters._

class BigQueryCaseClassGeneratorSpec
    extends WordSpec
    with Matchers
    with BeforeAndAfter
    with DiagrammedAssertions {

  val datasetId = "test_dataset"
  val outputDir = "src/test/resources"
  val outputPkg = "output.bq.testdataset"

  val expectFileDir = "src/test/resources/output/bq/testdataset/"
  val actualFileDir = "src/test/scala/output/bq/testdataset/"

  before {
    BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)
  }

  "class generator" should {
    "create classes from simple tables" in {
      val (expectLines, actualLines) = readLinesPair("SampleTable1Table.scala")
      expectLines shouldBe actualLines
    }
  }

  def readLinesPair(fileName: String) = {
    val path1       = Paths.get(expectFileDir + fileName)
    val expectLines = Files.readAllLines(path1, StandardCharsets.UTF_8).asScala

    val path2       = Paths.get(actualFileDir + fileName)
    val actualLines = Files.readAllLines(path2, StandardCharsets.UTF_8).asScala

    (expectLines, actualLines)
  }
}
