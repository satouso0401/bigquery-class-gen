package bq.classgen

import java.nio.file.Paths
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest._
import bq.classgen.TestTables._

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
    TestTables.createTable(datasetId)
    BigQueryCaseClassGenerator.run(datasetId, outputDir, outputPkg)
  }

  "class generator" should {
    "create classes from simple tables" in {

      val (expectLines, actualLines) = readLinesPair("SimpleTable.scala")
      expectLines shouldBe actualLines
    }

    "create classes from structured tables" in {

      val (expectLines, actualLines) = readLinesPair(STRUCTURED_TABLE.classFileName)
      expectLines shouldBe actualLines
    }

    "create classes from various type tables" in {

      val (expectLines, actualLines) = readLinesPair(VARIOUS_TYPE_TABLE.classFileName)
      expectLines shouldBe actualLines
    }

    "create classes from nullable and repeated tables" in {

      val (expectLines, actualLines) = readLinesPair(NULLABLE_AND_REPEATED_TABLE.classFileName)
      expectLines shouldBe actualLines
    }

    "create classes from nullable and repeated struct tables" in {

      val (expectLines, actualLines) = readLinesPair(NULLABLE_AND_REPEATED_STRUCT_TABLE.classFileName)
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
