package example

import zio.Chunk
import zio.dynamodb.{AttrMap, AttributeValue, Codec, Decoder, SchemaUtils}
import zio.schema.{DynamicValue, Schema, TypeId}

import java.time.Instant
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions

object SchemaParser {

  final case class resource_prefix(name: String) extends StaticAnnotation
  // uniquely identifies the record
  final case class id_field() extends StaticAnnotation
  final case class parent_field() extends StaticAnnotation

  // means the field could be used in dynamo queries as a parameter
  // the value should correspond to the GSI name
  final case class indexed(name: String) extends StaticAnnotation

  private def findIdField(fields: Chunk[Schema.Field[_, _]]): Option[Schema.Field[Any, String]] = {
    fields.find(_.annotations.exists(_.isInstanceOf[id_field]))
      .map(_.asInstanceOf[Schema.Field[Any, String]]) //todo: check if there is only one id field in the schema
  }

  def findResourcePrefix(schema: Schema[_]): Option[String] = {
    schema.annotations.collectFirst {
      case resource_prefix(name) => name
    }
  }


  trait SchemaProcessorForWrites {

    // it actually could be more complicated than that (ie conditional expression) maybe multiple writes
    def toAttrMap(input: DynamicValue): AttrMap
  }

  // just a typed option to test things out
  trait ProcessedSchemaTyped [T] {
    def resourcePrefix: String
    def toAttrMap(input: T): AttrMap
    def fromAttrMap(attrMap: AttrMap): T
  }

  implicit def toProcessor[T](implicit schema: Schema[T]): ProcessedSchemaTyped[T] = validateTyped(schema)

  def validateTyped[T](schema: Schema[T]): ProcessedSchemaTyped[T] = {
    val record = schema match {
      case record: Schema.Record[_] => record
      case other => throw new RuntimeException(s"Expected record, got $other")
    }

    val idField: Schema.Field[Any, String] = findIdField(record.fields).getOrElse(throw new RuntimeException("Id field not found for the schema")) //todo: add schema name
    val parentField: Schema.Field[Any, String] = record.fields.find(_.annotations.exists(_.isInstanceOf[parent_field]))
      .map(_.asInstanceOf[Schema.Field[Any, String]]).get
    val resourcePrefixValue: String = findResourcePrefix(schema).getOrElse(throw new RuntimeException("Resource prefix not found"))

    val otherFields = record.fields.filterNot(_.annotations.exists(_.isInstanceOf[id_field]))

    new ProcessedSchemaTyped[T] {
      override def toAttrMap(input: T): AttrMap = {
        val otherAttributes: Map[String, AttributeValue] = otherFields.map { field =>
          val value = field.get(input)
          val attrValue = value match {
            case s: String => AttributeValue(s)
            case i: Int => AttributeValue(i)
            case b: Boolean => AttributeValue(b)
            case other => throw new RuntimeException(s"Unsupported type: $other")
          }
          field.name -> attrValue
        }.toMap

        val attributes : Map[String, AttributeValue] = Map (
          "sk" -> AttributeValue(resourcePrefix + "#" + idField.get(input) + "#" + Instant.now().toString), //todo: add timestamp
          "pk" -> AttributeValue(parentField.get(input))
        ) ++ otherAttributes

        AttrMap(attributes)
      }

      override def fromAttrMap(attrMap: AttrMap): T = {
        val params = attrMap.map.keys.map(k => SchemaUtils.attributeValueString(k) -> attrMap.map(k)).toMap

        schema match {
          case s @ Schema.CaseClass3(id0, field1, field2, fiel3, _, annotations) =>
            val attributeValueMap = SchemaUtils.attributeValueMap(params)
            val dec = SchemaUtils.caseClass3Decoder(s)
              .apply(attributeValueMap)
              .getOrElse(throw new RuntimeException("Failed to parse"))
            dec
        }

      }

      override def resourcePrefix: String = {
        resourcePrefixValue
      }
    }


  }

  //a method that accepts schema
  // we return fields to access the fields
  // i need fields for accessing timestamp, sort key, and partition key
  def validate(schema: Schema[_]): SchemaProcessorForWrites = {

    val record = schema match {
      case record: Schema.Record[_] => record
      case other => throw new RuntimeException(s"Expected record, got $other")
    }

    val idField: Schema.Field[Any, String] = findIdField(record.fields).get

    new SchemaProcessorForWrites {
      override def toAttrMap(input: DynamicValue): AttrMap = {
        val x = schema.fromDynamic(input).getOrElse(throw new RuntimeException("Failed to parse"))
        val s: String = idField.get(x)
        AttrMap("id" -> s)
      }
    }
  }

}
