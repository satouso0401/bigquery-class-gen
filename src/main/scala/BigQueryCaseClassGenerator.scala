import java.io.{BufferedWriter, File, FileWriter}

import com.google.cloud.bigquery._

import scala.jdk.CollectionConverters._

object BigQueryCaseClassGenerator {

  val bigquery = BigQueryOptions.getDefaultInstance.getService

  case class FieldInfo(classField: String, mappingPair: String)
  case class StructInfo(classField: String,
                        classDef: Seq[String],
                        mappingPair: String,
                        mappingDef: Seq[String])

  // TODO パッケージ名の整理
  // TODO 名前が同じSTRUCTの case class が衝突しないようにテーブル毎に出力するファイルを変える
  // TODO 同じ構造のSTRUCTの場合を考慮して case class の重複削除の処理を入れる
  // TODO nullableなフィールドに対応する
  // TODO 繰り返しフィールドに対応する
  // TODO 日付型などの変換はおそらく今のままだと動かない
  // TODO generateFieldが大きいので分割する
  // TODO テストのやり方を考える コードの生成部分だけテストする？ テスト用のリポジトリを作る？ マルチプロジェクトにする？
  // TODO [やれたらやる] BQからの読み込み時のデータマッピング処理の追加


  def run(datasetId: String, outputDir: String, outputPkg: String) = {

    // table list
    val tableIdList =
      bigquery.listTables(datasetId).iterateAll().asScala.toList.map(_.getTableId.getTable)

    // generate code
    val codeList = for (tableId <- tableIdList) yield {
      val tableName = tableId.UCamel
      val table     = bigquery.getTable(datasetId, tableId)
      val schema    = table.getDefinition[TableDefinition]().getSchema
      val fieldList = schema.getFields.iterator().asScala.toList
      generateClass(tableName, fieldList)
    }

    // wrap with package and object
    val objectName = datasetId.UCamel
    val packageContainerCode =
      s"""
         |package $outputPkg
         |import java.util
         |import scala.jdk.CollectionConverters._
         |
         |object $objectName {
         |${codeList.mkString("\n")}
         |}
         |""".stripMargin

    writeFile(outputDir, outputPkg, objectName, packageContainerCode)

  }

  def generateClass(tableName: String, fields: Seq[Field]) = {

    val list = for (f: Field <- fields) yield {
      generateField(f)
    }

    val rootClassField = list
      .map {
        case Left(x)  => x.classField
        case Right(x) => x.classField
      }
      .mkString(", ")
    val rootClass = s"case class $tableName($rootClassField)"
    val nodeClass = list.collect { case Right(x) => x.classDef }.flatten
    val caseClass = "\n" + (rootClass +: nodeClass).mkString("\n") + "\n"

    val rootMappingPair = list
      .map {
        case Left(x)  => x.mappingPair
        case Right(x) => x.mappingPair
      }
      .mkString(", ")

    val rootMappingDef =
      s"""
         |object $tableName{
         |  implicit class ToBqRow(val x: $tableName) {
         |    def toBqRow: util.Map[String, Any] = { Map($rootMappingPair) }.asJava
         |  }
         |}
         |""".stripMargin

    val nodeMappingDef = list.collect { case Right(x) => x.mappingDef }.flatten
    val mappingDef     = "\n" + (rootMappingDef +: nodeMappingDef).mkString("\n") + "\n"

    caseClass + "\n" + mappingDef
  }

  def generateField(field: Field): Either[FieldInfo, StructInfo] = {
    if (field.getType.getStandardType == StandardSQLTypeName.STRUCT) {
      val fieldList = field.getSubFields.iterator().asScala.toSeq.map(x => generateField(x))

      // case class field
      val thisFieldName = field.getName.lCamel
      val thisFieldType = field.getName.UCamel
      val thisField     = s"$thisFieldName: $thisFieldType"

      // case class define
      val childFields = fieldList
        .map {
          case Left(x)  => x.classField
          case Right(x) => x.classField
        }
        .mkString(", ")
      val thisClass = s"case class $thisFieldType($childFields)"

      // child case class define list
      val childClassList = fieldList.collect { case Right(x) => x }.flatMap(_.classDef)

      // column mapping
      val thisMapDefName = field.getName.lCamel
      val thisMapPair    = s""""${field.getName}" -> $thisMapDefName(x.$thisFieldName)"""

      // column mapping function
      val childMapPair = fieldList
        .map {
          case Left(x)  => x.mappingPair
          case Right(x) => x.mappingPair
        }
        .mkString(", ")
      val thisMapDef = s"def $thisMapDefName(x: $thisFieldType) = { Map($childMapPair)}.asJava"

      // child mapping function list
      val childMapDefList = fieldList.collect { case Right(x) => x }.flatMap(_.mappingDef)

      Right(
        StructInfo(thisField,
                   childClassList :+ thisClass,
                   thisMapPair,
                   childMapDefList :+ thisMapDef))

    } else {

      import com.google.cloud.bigquery.StandardSQLTypeName._

      // case class field
      val thisFieldName = field.getName.lCamel
      val thisFieldType = field.getType.getStandardType match {
        case STRING    => "String"
        case INT64     => "Long"
        case FLOAT64   => "Double"
        case TIMESTAMP => "ZonedDateTime"
        case x         => throw new UnsupportedOperationException(s"$x field not supported")
      }
      val thisField = s"$thisFieldName: $thisFieldType"

      // column mapping
      val thisMapPair = s""""${field.getName}" -> x.$thisFieldName"""

      Left(FieldInfo(thisField, thisMapPair))
    }
  }

  def writeFile(outputDir: String, outputPkg: String, objectName: String, code: String) = {
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

  def camelCase(str: String) = {
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
