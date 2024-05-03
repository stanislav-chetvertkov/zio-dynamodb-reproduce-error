package configuration

import zio.Chunk
import zio.dynamodb.SchemaUtils.{Timestamp, Version}
import zio.dynamodb.{AttrMap, AttributeValue, SchemaUtils}
import zio.schema.{DynamicValue, Schema}

import java.time.Instant
import scala.annotation.StaticAnnotation
import scala.language.implicitConversions

object SchemaParser {
  val GSI_SK: String = "gsi_sk1"
  val GSI_PK: String = "gsi_pk1"
  val GSI_PK2: String = "gsi_pk2"
  val GSI_SK2: String = "gsi_sk2"
  val PK: String = "pk"
  val SK: String = "sk"
  val TIMESTAMP = "timestamp"
  val SEPARATOR = "#"
  val HISTORY = "history"
  val GSI_VALUES_PREFIX = "values"

  val GSI_INDEX_NAME = "gsi1"
  val GSI_INDEX_NAME2 = "gsi2"

  final case class resource_prefix(name: String) extends StaticAnnotation

  // uniquely identifies the record
  final case class id_field() extends StaticAnnotation

  final case class parent_field() extends StaticAnnotation


  case class IndexName[T](name: String)

  // means the field could be used in dynamo queries as a parameter
  // the value should correspond to the GSI name
  //todo: needs to have the resource prefix and the field prefix
  // actually the resource prefix is already available on the case class
  // field prefix should be helpful, maybe it's worth spliting between pk and sk
  // where pk is the resource prefix + field prefix and sk is the value
  final case class indexed[A](indexName: IndexName[A], pkName: String, skName: String) extends StaticAnnotation

  private def findIdField(fields: Chunk[Schema.Field[?, ?]]): Option[Schema.Field[Any, String]] = {
    fields.find(_.annotations.exists(_.isInstanceOf[id_field]))
      .map(_.asInstanceOf[Schema.Field[Any, String]]) //todo: check if there is only one id field in the schema
  }

  private def findResourcePrefix(schema: Schema[?]): Option[String] = schema.annotations.collectFirst {
    case resource_prefix(name) => name
  }

  // collect fields that have indexed annotation
  // return list of tuples (field, indexed)
  private def findIndexedFields[T](fields: Chunk[Schema.Field[T, ?]]): Chunk[(Schema.Field[T, ?], indexed[?])] = {
    val fieldsWithIndexed = fields.filter(_.annotations.exists(_.isInstanceOf[indexed[?]]))
      .map(field =>
        (field, field.annotations.collectFirst {
          case i: indexed[_] => i
        }.get)
      )

    fieldsWithIndexed
  }

  // just a typed option to test things out
  trait ProcessedSchemaTyped[T] {
    def resourcePrefix: String

    def parentId(input: T): String

    def resourceId(input: T): String

    def toAttrMap(input: T): AttrMap = toAttrMap(input, 1)

    def toAttrMap(input: T, version: Int, isHistory: Boolean = false): AttrMap

    def fromAttrMapWithTimestamp(attrMap: AttrMap): (T, Version, Timestamp)

    def fromAttrMap(attrMap: AttrMap): T = fromAttrMapWithTimestamp(attrMap)._1
  }


  implicit def toProcessor[T](implicit schema: Schema[T]): ProcessedSchemaTyped[T] = validate(schema)

  // fields for accessing timestamp, sort key, and partition key
  // validates the schema to make sure if has the required annotations and returns a processor
  def validate[T](schema: Schema[T]): ProcessedSchemaTyped[T] = {
    val record: Schema.Record[T] = schema match {
      case record: Schema.Record[_] => record
      case other => throw new RuntimeException(s"Expected record, got $other")
    }

    //todo: add schema name
    val idField: Schema.Field[Any, String] = findIdField(record.fields)
      .getOrElse(throw new RuntimeException("Id field not found for the schema"))

    val parentField: Schema.Field[Any, String] = record.fields.find(_.annotations.exists(_.isInstanceOf[parent_field]))
      .map(_.asInstanceOf[Schema.Field[Any, String]]).get
    val resourcePrefixValue: String = findResourcePrefix(schema).getOrElse(throw new RuntimeException("Resource prefix not found"))

    val otherFields = record.fields.filterNot(_.annotations.exists(_.isInstanceOf[id_field]))

    val indexedFields = findIndexedFields(record.fields)

    new ProcessedSchemaTyped[T] {
      def toAttrMap(input: T, version: Int, isHistoryRecord: Boolean = false): AttrMap = {
        val indexed = if (!isHistoryRecord) {
          indexedFields.map { case (field, indexed) =>
            val attrValue: AttributeValue = field.get(input) match {
              case s: String => AttributeValue[String](s)
              case i: Int => AttributeValue[Int](i)
              case b: Boolean => AttributeValue[Boolean](b)
              case other => throw new RuntimeException(s"Unsupported type: $other")
            }

            val pkEntry = indexed.pkName -> attrValue
            val skEntry = indexed.skName -> attrValue
            List(pkEntry, skEntry)
          }.flatten.toMap
        } else {
          Map.empty[String, AttributeValue]
        }

        val otherAttributes: Map[String, AttributeValue] = otherFields.map { field =>
          val fieldValue = field.get(input)
          val attrValue = fieldValue match { //is it compile time safe?
            case s: String => AttributeValue[String](s)
            case i: Int => AttributeValue[Int](i)
            case b: Boolean => AttributeValue[Boolean](b)
            case other => throw new RuntimeException(s"Unsupported type: $other")
          }
          field.name -> attrValue
        }.toMap

        val now = Instant.now().toString //todo: should be passed as an argument
        val id = idField.get(input)
        val attributes: Map[String, AttributeValue] = Map(
          SK -> {
            if (isHistoryRecord) {
              val compositeKey = List(HISTORY, resourcePrefixValue, id, version).mkString(SEPARATOR)
              AttributeValue(compositeKey)
            } else {
              AttributeValue(resourcePrefix + SEPARATOR + id)
            }
          },
          PK -> AttributeValue(parentField.get(input)),
          GSI_PK -> AttributeValue(resourcePrefix),
          GSI_SK -> {
            if (isHistoryRecord) {
              // I'm not sure whether it's better to have version in the sort key or the timestamp
              // the problem with version is that it has to be sortable that it means it should be padded with 0s on the left
              // like 0001, 0002, 0003, etc otherwise it will be sorted as 1, 10, 11, 2, 3
              // though It could be sorted on the client after receiving the data that adds additional work
              val compositeKey = List(HISTORY, id, version).mkString(SEPARATOR)
              AttributeValue(compositeKey)
            } else {
              val compositeKey = List("values", id, now).mkString(SEPARATOR)
              AttributeValue(compositeKey)
            }
          },
          TIMESTAMP -> AttributeValue(now),
          "version" -> AttributeValue(version)
        ) ++ otherAttributes ++ indexed

        AttrMap(attributes)
      }

      override def fromAttrMapWithTimestamp(attrMap: AttrMap): (T, Version, Timestamp) = {
        val params = attrMap.map.keys.map(k => SchemaUtils.attributeValueString(k) -> attrMap.map(k)).toMap
        schema match {
          case s@Schema.CaseClass3(_, _, _, _, _, _) =>
            val attributeValueMap = SchemaUtils.attributeValueMap(params)
            SchemaUtils.caseClass3Decoder(s)
              .apply(attributeValueMap)
              .getOrElse(throw new RuntimeException(s"Failed to parse $params"))
          case s@Schema.CaseClass4(_, _, _, _, _, _, _) =>
            val attributeValueMap = SchemaUtils.attributeValueMap(params)
            SchemaUtils.caseClass4Decoder(s)
              .apply(attributeValueMap)
              .getOrElse(throw new RuntimeException(s"Failed to parse $params"))
          case other => throw new RuntimeException(s"Unsupported schema: $other")
        }
      }

      override def resourcePrefix: String = resourcePrefixValue

      override def parentId(input: T): Timestamp = parentField.get(input)

      override def resourceId(input: T): String = idField.get(input)
    }
  }

}
