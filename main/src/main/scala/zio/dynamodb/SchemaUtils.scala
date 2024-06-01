package zio.dynamodb

import configuration.ConfigSchemaCodec
import configuration.ConfigSchemaCodec.{Timestamp, Version, VersionPlaceholder, id_field}
import configuration.TableStructure.*
import zio.dynamodb.Codec.Decoder.{ContainerField, decoder}
import zio.dynamodb.DynamoDBError.ItemError.DecodingError
import zio.prelude.ForEachOps
import zio.schema.Schema

object SchemaUtils {
  def attributeValueMap(input: Map[AttributeValue.String, AttributeValue]): AttributeValue.Map = AttributeValue.Map(input)

  def attributeValueString(input: String): AttributeValue.String = AttributeValue.String(input)

  private type DecoderWithTimeStamp[+A] = AttributeValue => Either[DynamoDBError, (A, Version, Timestamp)]

  def caseClass3Decoder[A, B, C, Z](schema: Schema.CaseClass3[A, B, C, Z]): DecoderWithTimeStamp[Z] = { (av: AttributeValue) =>
    val dec = SchemaUtils.decodeInnerFields(av, schema.field1, schema.field2, schema.field3)
    val k = dec.map(_._1).map { xs =>
      schema.construct(xs(0).asInstanceOf[A], xs(1).asInstanceOf[B], xs(2).asInstanceOf[C])
    }
    val version: Version = dec.map(_._2).getOrElse(ConfigSchemaCodec.VersionPlaceholder)
    k.map((_, version, dec.map(_._3).getOrElse("".asInstanceOf[Timestamp])))
  }

  def caseClass4Decoder[A, B, C, D, Z](schema: Schema.CaseClass4[A, B, C, D, Z]): DecoderWithTimeStamp[Z] = { (av: AttributeValue) =>
    val dec: Either[DynamoDBError, (List[Any], Version, Timestamp)] =
      SchemaUtils.decodeInnerFields(av, schema.field1, schema.field2, schema.field3, schema.field4)

    val k = dec.map(_._1).map { xs =>
      schema.construct(xs(0).asInstanceOf[A], xs(1).asInstanceOf[B], xs(2).asInstanceOf[C], xs(3).asInstanceOf[D])
    }
    k.map((_, dec.map(_._2).getOrElse(VersionPlaceholder), (dec.map(_._3).getOrElse("").asInstanceOf[Timestamp])))
  }

  private def decodeInnerFields(av: AttributeValue, fields: Schema.Field[?, ?]*): Either[DynamoDBError, (List[Any], Version, Timestamp)] =
    av match {
      case AttributeValue.Map(map) =>
        val versionOpt: Either[DecodingError, Version] = map.get(AttributeValue.String("version")).map(_.asInstanceOf[AttributeValue.Number].value.toInt.asInstanceOf[Version])
          .toRight(DecodingError(s"field 'version' not found in $av"))

        val timestamp: Timestamp = map.get(AttributeValue.String("timestamp"))
          .map(_.asInstanceOf[AttributeValue.String].value.asInstanceOf[Timestamp]).getOrElse("".asInstanceOf[Timestamp])

        val myVar = fields.toList.forEach {
          case Schema.Field(key, schema, annotations, _, _, _) =>
            if (annotations.exists(_.isInstanceOf[id_field])) {
              val maybeValue: Option[AttributeValue] = map.get(AttributeValue.String(SK))
              val r = maybeValue.get match {
                case AttributeValue.String(value) =>
                  value.split(SEPARATOR).toList match {
                    case "history" :: prefix :: id :: tail => // history subsection
                      Some(id).toRight(DecodingError(s"field 'sk' not found in $av"))
                    case prefix :: id :: tail =>
                      Some(id).toRight(DecodingError(s"field 'sk' not found in $av"))
                    case _ => Left(DecodingError(s"field 'sk' not found in $av"))
                  }
                case _ => Left(DecodingError(s"field 'sk' not found in $av"))
              }

              val either = for {decoded <- r} yield decoded

              if (maybeValue.isEmpty) {
                ContainerField.containerField(schema) match {
                  case ContainerField.Optional => Right(None)
                  case ContainerField.Sequence => Right(List.empty)
                  case ContainerField.Map => Right(Map.empty)
                  case ContainerField.Set => Right(Set.empty)
                  case ContainerField.Scalar => either
                }
              } else {
                either
              }
            } else {
              val dec = decoder(schema)
              val maybeValue = map.get(AttributeValue.String(key))
              val maybeDecoder = maybeValue.map(dec).toRight(DecodingError(s"field '$key' not found in $av"))

              val either: Either[DynamoDBError, Any] = for {
                decoder <- maybeDecoder
                decoded <- decoder
              } yield decoded

              if (maybeValue.isEmpty) {
                ContainerField.containerField(schema) match {
                  case ContainerField.Optional => Right(None)
                  case ContainerField.Sequence => Right(List.empty)
                  case ContainerField.Map => Right(Map.empty)
                  case ContainerField.Set => Right(Set.empty)
                  case ContainerField.Scalar => either
                }
              } else {
                either
              }
            }
        }

        myVar.map(_.toList).map((_, versionOpt.getOrElse(VersionPlaceholder), timestamp))

      case _ =>
        Left(DecodingError(s"$av is not an AttributeValue.Map"))
    }


  def toItem[A](a: A)(implicit schema: Schema[A]): Item =
    FromAttributeValue.attrMapFromAttributeValue
      .fromAttributeValue(AttributeValue.encode(a)(schema))
      .getOrElse(throw new Exception(s"error encoding $a"))

}
