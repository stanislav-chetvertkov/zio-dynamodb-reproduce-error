package zio.dynamodb

import example.SchemaParser.id_field
import zio.dynamodb.Codec.Decoder.{ContainerField, decoder}
import zio.dynamodb.DynamoDBError.DecodingError
import zio.prelude.ForEachOps
import zio.schema.Schema

object SchemaUtils {

  def attributeValueMap(input: Map[AttributeValue.String, AttributeValue]): AttributeValue.Map =
    AttributeValue.Map(input)

  def attributeValueString(input: String): AttributeValue.String =
    AttributeValue.String(input)

  def fromItem[A: Schema](item: Item): Either[DynamoDBError, A] = {
    val av = ToAttributeValue.attrMapToAttributeValue.toAttributeValue(item)
    av.decode(Schema[A])
  }


  def caseClass3Decoder[A, B, C, Z](schema: Schema.CaseClass3[A, B, C, Z]): Decoder[Z] =  { (av: AttributeValue) =>
    SchemaUtils.decodeFields2(av, schema.field1,schema.field2,schema.field3).map { xs =>
      schema.construct(xs(0).asInstanceOf[A], xs(1).asInstanceOf[B], xs(2).asInstanceOf[C])
    }
  }

  def decodeFields2(av: AttributeValue,
                    fields: Schema.Field[_, _]*
                   ): Either[DynamoDBError, List[Any]] =
    av match {
      case AttributeValue.Map(map) =>
        fields.toList.forEach {
            case Schema.Field(key, schema, annotations, _, _, _) =>
              if (annotations.exists(_.isInstanceOf[id_field])) {
                val maybeValue: Option[AttributeValue] = map.get(AttributeValue.String("sk"))
                val r: Either[DynamoDBError, Any] = maybeValue.get match {
                  case AttributeValue.String(value) =>
                    value.split("#").toList match {
                      case prefix :: id :: timestamp :: Nil =>
                        val maybeDecoder = Some(id).toRight(DecodingError("field 'sk' not found in $av"))
                        maybeDecoder
                      case _ => Left(DecodingError(s"field 'sk' not found in $av"))
                    }
                  case _ => Left(DecodingError(s"field 'sk' not found in $av"))
                }

                val either: Either[DynamoDBError, Any] = for {
                  decoded <- r
                } yield decoded

                if (maybeValue.isEmpty)
                  ContainerField.containerField(schema) match {
                    case ContainerField.Optional => Right(None)
                    case ContainerField.List => Right(List.empty)
                    case ContainerField.Map => Right(Map.empty)
                    case ContainerField.Set => Right(Set.empty)
                    case ContainerField.Scalar => either
                  }
                else
                  either

              } else {
                val dec = decoder(schema)
                val maybeValue = map.get(AttributeValue.String(key))
                val maybeDecoder = maybeValue.map(dec).toRight(DecodingError(s"field '$key' not found in $av"))
                val either: Either[DynamoDBError, Any] = for {
                  decoder <- maybeDecoder
                  decoded <- decoder
                } yield decoded

                if (maybeValue.isEmpty)
                  ContainerField.containerField(schema) match {
                    case ContainerField.Optional => Right(None)
                    case ContainerField.List => Right(List.empty)
                    case ContainerField.Map => Right(Map.empty)
                    case ContainerField.Set => Right(Set.empty)
                    case ContainerField.Scalar => either
                  }
                else
                  either
              }



          }
          .map(_.toList)
      case _                       =>
        Left(DecodingError(s"$av is not an AttributeValue.Map"))
    }


}
