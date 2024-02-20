package zio.dynamodb

import example.SchemaParser.id_field
import zio.dynamodb.Codec.Decoder.{ContainerField, decoder}
import zio.dynamodb.DynamoDBError.DecodingError
import zio.prelude.ForEachOps
import zio.schema.Schema

object SchemaUtils {

  type Timestamp = String

  def attributeValueMap(input: Map[AttributeValue.String, AttributeValue]): AttributeValue.Map =
    AttributeValue.Map(input)

  def attributeValueString(input: String): AttributeValue.String =
    AttributeValue.String(input)


  type DecoderWithTimeStamp[+A] = AttributeValue => Either[DynamoDBError, (A, Timestamp)]

  def caseClass3Decoder[A, B, C, Z](schema: Schema.CaseClass3[A, B, C, Z]): DecoderWithTimeStamp[Z] = { (av: AttributeValue) =>
    val dec = SchemaUtils.decodeInnerFields(av, schema.field1, schema.field2, schema.field3)
    val k = dec.map(_._1).map { xs =>
      schema.construct(xs(0).asInstanceOf[A], xs(1).asInstanceOf[B], xs(2).asInstanceOf[C])
    }
    k.map((_, dec.map(_._2).getOrElse("")))
  }

  def caseClass4Decoder[A, B, C, D, Z](schema: Schema.CaseClass4[A, B, C, D, Z]): DecoderWithTimeStamp[Z] = { (av: AttributeValue) =>
    val dec = SchemaUtils.decodeInnerFields(av, schema.field1, schema.field2, schema.field3, schema.field4)
    val k = dec.map(_._1).map { xs =>
      schema.construct(xs(0).asInstanceOf[A], xs(1).asInstanceOf[B], xs(2).asInstanceOf[C], xs(3).asInstanceOf[D])
    }
    k.map((_, dec.map(_._2).getOrElse("")))
  }

  private def decodeInnerFields(av: AttributeValue, fields: Schema.Field[_, _]*): Either[DynamoDBError, (List[Any], Timestamp)] =
    av match {
      case AttributeValue.Map(map) =>
        var timestampOpt: Option[Timestamp] = Option.empty // using var to prototype quicker, consider changing to val
        val myVar = fields.toList.forEach {
          case Schema.Field(key, schema, annotations, _, _, _) =>
            if (annotations.exists(_.isInstanceOf[id_field])) {
              val maybeValue: Option[AttributeValue] = map.get(AttributeValue.String("sk"))
              val r = maybeValue.get match {
                case AttributeValue.String(value) =>
                  value.split("#").toList match {
                    case prefix :: id :: timestamp :: Nil =>
                      if (timestampOpt.isEmpty) {
                        timestampOpt = Some(timestamp)
                      }

                      val maybeDecoder = Some(id).toRight(DecodingError("field 'sk' not found in $av"))
                      maybeDecoder
                    case _ => Left(DecodingError(s"field 'sk' not found in $av"))
                  }
                case _ => Left(DecodingError(s"field 'sk' not found in $av"))
              }

              val either = for {decoded <- r} yield decoded

              if (maybeValue.isEmpty) {
                ContainerField.containerField(schema) match {
                  case ContainerField.Optional => Right(None)
                  case ContainerField.List => Right(List.empty)
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
                  case ContainerField.List => Right(List.empty)
                  case ContainerField.Map => Right(Map.empty)
                  case ContainerField.Set => Right(Set.empty)
                  case ContainerField.Scalar => either
                }
              } else {
                either
              }
            }
        }

        myVar.map(_.toList).map((_, timestampOpt.getOrElse("")))

      case _ =>
        Left(DecodingError(s"$av is not an AttributeValue.Map"))
    }


}
