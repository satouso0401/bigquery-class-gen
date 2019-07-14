package bq.classgen

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Path, Paths}

import com.google.cloud.bigquery.Field.Mode.{NULLABLE, REPEATED}
import com.google.cloud.bigquery._
import org.scalafmt.interfaces.Scalafmt

import scala.collection.JavaConverters._

object BigQueryCaseClassGenerator {
  val NothingOverwriteType: PartialFunction[StandardSQLTypeName, String] = {
    case _ if false => ""
  }
  val NothingOverwriteClassToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
    case _ if false => ""
  }
  val NothingOverwriteFunctorToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
    case _ if false => ""
  }

  /**
    * Create BigQueryCaseClassGenerator
    * @param importClass import class
    * @param overwriteType customize the mapping of BigQuery type and class field type
    * @param overwriteClassToRow customize class field to BigQuery row conversion
    * @param overwriteFunctorToRow customize functor field to BigQuery row conversion
    * @return BigQueryCaseClassGenerator
    */
  def apply(importClass: Seq[String] = Nil,
            overwriteType: PartialFunction[StandardSQLTypeName, String] = NothingOverwriteType,
            overwriteClassToRow: PartialFunction[(StandardSQLTypeName, String), String] =
              NothingOverwriteClassToRow,
            overwriteFunctorToRow: PartialFunction[(StandardSQLTypeName, String), String] =
              NothingOverwriteFunctorToRow) =
    new BigQueryCaseClassGenerator(importClass,
                                   overwriteType,
                                   overwriteClassToRow,
                                   overwriteFunctorToRow)

  lazy val scalafmt = Scalafmt
    .create(this.getClass.getClassLoader)
    .withRespectVersion(false)
    .withDefaultVersion("2.0.0")

  def format(code: String) = {
    val config: Path = Paths.get(".scalafmt.conf")

    if (Files.exists(config)) {
      scalafmt.format(config, Paths.get("NotExistFile.scala"), code)
    } else code

  }
}

/**
  * This class makes case class from BigQuery
  * @param importClass import class
  * @param overwriteType customize the mapping of BigQuery type and class field type
  * @param overwriteClassToRow customize class field to BigQuery row conversion
  * @param overwriteFunctorToRow customize functor field to BigQuery row conversion
  */
class BigQueryCaseClassGenerator(
    importClass: Seq[String],
    overwriteType: PartialFunction[StandardSQLTypeName, String],
    overwriteClassToRow: PartialFunction[(StandardSQLTypeName, String), String],
    overwriteFunctorToRow: PartialFunction[(StandardSQLTypeName, String), String]) {

  import bq.classgen.BigQueryCaseClassGenerator.format

  case class FieldInfo(field: String, mappingPair: String)
  case class StructInfo(field: String,
                        structDef: Seq[String],
                        mappingPair: String,
                        mappingDef: Seq[String])

  // TODO case classからBQへの登録用のオブジェクトへ変換する際に、ScalaのMapを使っているが、JavaのMapに直接変換した方が良いかもしれない

  /**
    * Generate code of case class from BigQuery schema.
    * @param datasetId dataset to reference
    * @param outputDir case class output directory
    * @param outputPkg package to which the case class belongs
    * @param separatePackagesByTable option to change output package when struct with same name exists
    * @param bigquery BigQuery service object
    */
  def run(datasetId: String,
          outputDir: String,
          outputPkg: String,
          separatePackagesByTable: Boolean = false)(implicit bigquery: BigQuery): Unit = {

    // table list
    val tableIdList =
      bigquery.listTables(datasetId).iterateAll().asScala.toList.map(_.getTableId.getTable)

    for (tableId <- tableIdList) yield {

      // generate code
      val tableName = tableId.UCamel
      val table     = bigquery.getTable(datasetId, tableId)
      val schema    = table.getDefinition[TableDefinition]().getSchema
      val fieldList = schema.getFields.iterator().asScala.toList

      val codeBody = generateClass(tableName, fieldList)

      // write file
      val objectName = tableName
      val pkg        = if (separatePackagesByTable) s"$outputPkg.${tableName.toLowerCase}" else outputPkg
      val importCode = importClass.map(x => s"import $x").mkString("\n")
      val packageContainerCode =
        s"""package $pkg
           |
           |import java.time.format.DateTimeFormatter
           |import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
           |import java.util.Base64
           |import scala.collection.JavaConverters._
           |
           |$importCode
           |
           |$codeBody
           |""".stripMargin

      val formattedCode = format(packageContainerCode)

      writeFile(outputDir, pkg, objectName, formattedCode)
    }

  }

  private def generateClass(tableName: String, bqFields: Seq[Field]): String = {

    val list = for (f: Field <- bqFields) yield {
      generateField(f)
    }

    val rootClassField = list
      .map {
        case Left(x)  => x.field
        case Right(x) => x.field
      }
      .mkString(", ")
    val rootClass = s"case class $tableName($rootClassField)"
    val nodeClass = list.collect { case Right(x) => x.structDef }.flatten
    val caseClass = (rootClass +: nodeClass).distinct.mkString("\n")

    val rootMappingPair = list
      .map {
        case Left(x)  => x.mappingPair
        case Right(x) => x.mappingPair
      }
      .mkString(", ")

    val nodeMappingDef = list.collect { case Right(x) => x.mappingDef }.flatten
    val mappingDef =
      s"""
         |object $tableName{
         |  implicit class ToBqRow(val x: $tableName) {
         |    def toBqRow = { Map($rootMappingPair) }.asJava
         |  }
         |  ${nodeMappingDef.distinct.mkString("\n  ")}
         |}
         |""".stripMargin

    caseClass + "\n" + mappingDef
  }

  private def generateField(bqField: Field): Either[FieldInfo, StructInfo] = {
    if (bqField.getType.getStandardType == StandardSQLTypeName.STRUCT) {
      val childFieldList = bqField.getSubFields.iterator().asScala.toSeq.map(x => generateField(x))
      Right(generateStruct(bqField.getName, bqField.getMode, childFieldList))

    } else {

      val bqFieldName = bqField.getName
      val bqFieldType = bqField.getType.getStandardType
      val bqFieldMode = bqField.getMode

      Left(generateBasicField(bqFieldName, bqFieldType, bqFieldMode))
    }
  }

  private def generateStruct(bqFieldName: String,
                             bqFieldMode: Field.Mode,
                             childFieldList: Seq[Either[FieldInfo, StructInfo]]) = {
    val fieldName = bqFieldName.lCamel
    val fieldType = bqFieldName.UCamel

    // generate case class code
    val modeWrappedType = bqFieldMode match {
      case NULLABLE => s"Option[$fieldType]"
      case REPEATED => s"Seq[$fieldType]"
      case _        => fieldType
    }
    val thisField = s"$fieldName: $modeWrappedType"
    val thisClass = {
      val childFields = childFieldList
        .map {
          case Left(x)  => x.field
          case Right(x) => x.field
        }
        .mkString(", ")
      s"case class $fieldType($childFields)"
    }
    val childClassList = childFieldList.collect { case Right(x) => x }.flatMap(_.structDef)

    // generate mapping def
    val mappingDefName = bqFieldName.lCamel
    val thisMappingPair = bqFieldMode match {
      case NULLABLE => s""""$bqFieldName" -> x.$fieldName.map($mappingDefName).getOrElse(null)"""
      case REPEATED => s""""$bqFieldName" -> x.$fieldName.map($mappingDefName).asJava"""
      case _        => s""""$bqFieldName" -> $mappingDefName(x.$fieldName)"""
    }

    val thisMappingDef = {
      val childMappingPair = childFieldList
        .map {
          case Left(x)  => x.mappingPair
          case Right(x) => x.mappingPair
        }
        .mkString(", ")
      s"def $mappingDefName(x: $fieldType) = { Map($childMappingPair)}.asJava"
    }
    val childMapDefList = childFieldList.collect { case Right(x) => x }.flatMap(_.mappingDef)

    StructInfo(
      thisField,
      childClassList :+ thisClass,
      thisMappingPair,
      childMapDefList :+ thisMappingDef
    )
  }

  private def generateBasicField(bqFieldName: String,
                                 bqFieldType: StandardSQLTypeName,
                                 bqFieldMode: Field.Mode) = {
    import com.google.cloud.bigquery.Field.Mode._
    import com.google.cloud.bigquery.StandardSQLTypeName._

    // case class field
    val fieldName = bqFieldName.lCamel

    val defaultType: PartialFunction[StandardSQLTypeName, String] = {
      case INT64     => "Long"
      case NUMERIC   => "Long"
      case FLOAT64   => "Double"
      case BOOL      => "Boolean"
      case STRING    => "String"
      case BYTES     => "Array[Byte]"
      case DATE      => "LocalDate"
      case DATETIME  => "LocalDateTime"
      case TIME      => "LocalTime"
      case TIMESTAMP => "ZonedDateTime"
      case x         => throw new UnsupportedOperationException(s"$x field not supported")
    }

    val typeMatcher = overwriteType orElse defaultType
    val fieldType   = typeMatcher(bqFieldType)

    val modeWrappedType = bqFieldMode match {
      case NULLABLE => s"Option[$fieldType]"
      case REPEATED => s"Seq[$fieldType]"
      case _        => fieldType
    }
    val thisField = s"$fieldName: $modeWrappedType"

    val defaultClassToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
      case (INT64, fn)     => s"""x.$fn"""
      case (NUMERIC, fn)   => s"""x.$fn"""
      case (FLOAT64, fn)   => s"""x.$fn"""
      case (BOOL, fn)      => s"""x.$fn"""
      case (STRING, fn)    => s"""x.$fn"""
      case (BYTES, fn)     => s"""Base64.getEncoder.encodeToString(x.$fn)"""
      case (DATE, fn)      => s"""x.$fn.format(DateTimeFormatter.ISO_LOCAL_DATE)"""
      case (DATETIME, fn)  => s"""x.$fn.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)"""
      case (TIME, fn)      => s"""x.$fn.format(DateTimeFormatter.ISO_LOCAL_TIME)"""
      case (TIMESTAMP, fn) => s"""x.$fn.toInstant.getEpochSecond"""
      case x               => throw new UnsupportedOperationException(s"$x field not supported")
    }

    val classToRowMatcher = overwriteClassToRow orElse defaultClassToRow

    val defultFunctorToRow: PartialFunction[(StandardSQLTypeName, String), String] = {
      case (INT64, fn)     => s"x.$fn"
      case (NUMERIC, fn)   => s"x.$fn"
      case (FLOAT64, fn)   => s"x.$fn"
      case (BOOL, fn)      => s"x.$fn"
      case (STRING, fn)    => s"x.$fn"
      case (BYTES, fn)     => s"x.$fn.map(Base64.getEncoder.encodeToString)"
      case (DATE, fn)      => s"x.$fn.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE))"
      case (DATETIME, fn)  => s"x.$fn.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))"
      case (TIME, fn)      => s"x.$fn.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME))"
      case (TIMESTAMP, fn) => s"x.$fn.map(_.toInstant.getEpochSecond)"
      case x               => throw new UnsupportedOperationException(s"$x field not supported")
    }

    val functorToRowMatcher = overwriteFunctorToRow orElse defultFunctorToRow

    // column mapping
    val mappingFunc =
      bqFieldMode match {
        case NULLABLE | REPEATED =>
          val func = functorToRowMatcher((bqFieldType, fieldName))
          if (bqFieldMode == NULLABLE) func + ".getOrElse(null)" else func + ".asJava"
        case _ => classToRowMatcher((bqFieldType, fieldName))

      }

    val thisMapPair = s""""$bqFieldName" -> $mappingFunc"""

    FieldInfo(thisField, thisMapPair)
  }

  private def writeFile(outputDir: String,
                        outputPkg: String,
                        objectName: String,
                        code: String): Unit = {
    val folder = outputDir + "/" + outputPkg.replace(".", "/") + "/"
    new File(folder).mkdirs()
    val file = new File(folder + objectName + ".scala")
    if (!file.exists()) {
      file.createNewFile()
    }
    val fw = new FileWriter(file.getAbsoluteFile)
    val bw = new BufferedWriter(fw)
    bw.write(code)
    bw.close()

  }

  private def camelCase(str: String) = {
    str.toLowerCase.split("_").map(_.capitalize).mkString("")
  }

  implicit class CamelCaseUtil(val str: String) {
    def lCamel: String = {
      val s = camelCase(str)
      s(0).toString.toLowerCase + s.tail
    }
    def UCamel: String = camelCase(str)
  }

}
