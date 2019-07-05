package bq.classgen

import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery.Schema

object BqJsonToBqSchema {
  private val parser = new JacksonFactory()

  private def dtoTableSchemaToBqSchema(dtoSchema: TableSchema): Schema = {
    val fromPbMethod =
      classOf[Schema]
        .getDeclaredMethods
        .toIterable
        .find(method => method.getName == "fromPb")
        .get

    fromPbMethod.setAccessible(true)
    fromPbMethod.invoke(null, dtoSchema).asInstanceOf[Schema]
  }

  def convert(jsonSchemaString: String): Schema = {
    val fieldsListDto =
      parser
        .createJsonParser(jsonSchemaString)
        .parseArray(classOf[java.util.ArrayList[TableFieldSchema]], classOf[TableFieldSchema])
        .asInstanceOf[java.util.ArrayList[TableFieldSchema]]

    val schemaDto =  new TableSchema()
    schemaDto.setFields(fieldsListDto)

    dtoTableSchemaToBqSchema(schemaDto)
  }
}
