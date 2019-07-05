import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Path, Paths}

import com.google.cloud.bigquery.Field.Mode.{NULLABLE, REPEATED}
import com.google.cloud.bigquery._
import org.scalafmt.interfaces.Scalafmt

import scala.collection.JavaConverters._

object BigQueryCaseClassGenerator {

  val bigquery = BigQueryOptions.getDefaultInstance.getService

  case class FieldInfo(field: String, mappingPair: String)
  case class StructInfo(field: String,
                        structDef: Seq[String],
                        mappingPair: String,
                        mappingDef: Seq[String])

  // TODO パッケージ名の整理
  // TODO テストのやり方を考える コードの生成部分だけテストする？ テスト用のリポジトリを作る？ マルチプロジェクトにする？
  // TODO マッピング用の関数はScalaのMapではなくJavaのMapに直接変換した方がパフォーマンス上有利かもしれない
  // TODO マッピングする型の組みわせをカスタマイズできるようにする
  // TODO [やれたらやる] BQからの読み込み時のデータマッピング処理の追加

  def run(datasetId: String, outputDir: String, outputPkg: String): Unit = {

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
      val packageContainerCode =
        s"""package $outputPkg
           |import java.time.format.DateTimeFormatter
           |import java.time.{LocalDate, LocalDateTime, LocalTime, ZonedDateTime}
           |import java.util.Base64
           |import scala.collection.JavaConverters._
           |
           |$codeBody
           |""".stripMargin

      val formattedCode = format(packageContainerCode)

      writeFile(outputDir, outputPkg, objectName, formattedCode)
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

  def generateField(bqField: Field): Either[FieldInfo, StructInfo] = {
    if (bqField.getType.getStandardType == StandardSQLTypeName.STRUCT) {
      val childFieldList = bqField.getSubFields.iterator().asScala.toSeq.map(x => generateField(x))

      val fieldName = bqField.getName.lCamel
      val fieldType = bqField.getName.UCamel

      // generate case class code
      val modeWrappedType = bqField.getMode match {
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
      val bqFieldName    = bqField.getName
      val mappingDefName = bqField.getName.lCamel
      val thisMappingPair = bqField.getMode match {
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

      Right(
        StructInfo(
          thisField,
          childClassList :+ thisClass,
          thisMappingPair,
          childMapDefList :+ thisMappingDef
        )
      )

    } else {
      import com.google.cloud.bigquery.StandardSQLTypeName._
      import com.google.cloud.bigquery.Field.Mode._

      // case class field
      val fieldName = bqField.getName.lCamel
      val fieldType = bqField.getType.getStandardType match {
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
      val modeWrappedType = bqField.getMode match {
        case NULLABLE => s"Option[$fieldType]"
        case REPEATED => s"Seq[$fieldType]"
        case _        => fieldType
      }
      val thisField = s"$fieldName: $modeWrappedType"

      // column mapping

      val mappingFunc =
        bqField.getMode match {
          case NULLABLE | REPEATED =>
            val func = bqField.getType.getStandardType match {
              case INT64 | NUMERIC | FLOAT64 | BOOL | STRING => s"x.$fieldName"
              case BYTES                                     => s"x.$fieldName.map(y => Base64.getEncoder.encodeToString(y))"
              case DATE                                      => s"x.$fieldName.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE))"
              case DATETIME                                  => s"x.$fieldName.map(_.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))"
              case TIME                                      => s"x.$fieldName.map(_.format(DateTimeFormatter.ISO_LOCAL_TIME))"
              case TIMESTAMP                                 => s"x.$fieldName.map(_.toInstant.getEpochSecond)"
              case x                                         => throw new UnsupportedOperationException(s"$x field not supported")
            }
            if (bqField.getMode == NULLABLE) func + ".getOrElse(null)" else func + ".asJava"
          case _ =>
            bqField.getType.getStandardType match {
              case INT64 | NUMERIC | FLOAT64 | BOOL | STRING => s"""x.$fieldName"""
              case BYTES                                     => s"""Base64.getEncoder.encodeToString(x.$fieldName)"""
              case DATE                                      => s"""x.$fieldName.format(DateTimeFormatter.ISO_LOCAL_DATE)"""
              case DATETIME                                  => s"""x.$fieldName.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)"""
              case TIME                                      => s"""x.$fieldName.format(DateTimeFormatter.ISO_LOCAL_TIME)"""
              case TIMESTAMP                                 => s"""x.$fieldName.toInstant.getEpochSecond"""
              case x                                         => throw new UnsupportedOperationException(s"$x field not supported")
            }

        }

      val thisMapPair = s""""${bqField.getName}" -> $mappingFunc"""

      Left(FieldInfo(thisField, thisMapPair))
    }
  }

  private def structField(fieldName: String, fieldType: String) = s"$fieldName: $fieldType"

  private def structClass(fieldType: String, childFieldList: Seq[Either[FieldInfo, StructInfo]]) = {
    val childFields = childFieldList
      .map {
        case Left(x)  => x.field
        case Right(x) => x.field
      }
      .mkString(", ")
    s"case class $fieldType($childFields)"
  }

  private def structMappingPair(bqFieldName: String, mappingDefName: String, fieldName: String) =
    s""""$bqFieldName" -> $mappingDefName(x.$fieldName)"""

  private def structMappingDef(mappingDefName: String,
                               fieldType: String,
                               childFieldList: Seq[Either[FieldInfo, StructInfo]]) = {
    val childMappingPair = childFieldList
      .map {
        case Left(x)  => x.mappingPair
        case Right(x) => x.mappingPair
      }
      .mkString(", ")
    s"def $mappingDefName(x: $fieldType) = { Map($childMappingPair)}.asJava"
  }

  private def format(code: String) = {
    val scalafmt     = Scalafmt.create(this.getClass.getClassLoader)
    val config: Path = Paths.get(".scalafmt.conf")

    if (Files.exists(config)) {
      scalafmt
        .withRespectVersion(false)
        .withDefaultVersion("2.0.0")
        .format(config, Paths.get("NotExistFile.scala"), code)
    } else code

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
