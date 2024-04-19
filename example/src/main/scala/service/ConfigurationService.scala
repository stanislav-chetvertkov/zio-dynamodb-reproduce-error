package service

import example.SchemaParser.{GSI_INDEX_NAME2, GSI_PK2, GSI_SK2, id_field, indexed, parent_field, resource_prefix}
import example.dao.Repository
import zio.schema.Schema.Field
import zio.schema.Schema.Field.WithFieldName
import zio.schema.annotation.validate
import zio.schema.codec.JsonCodec
import zio.schema.validation.{Predicate, Validation}
import zio.schema.{DeriveSchema, Schema}
import zio.{ZIO, ZLayer}

case class ConfigurationService(repo: Repository) {

  import ConfigurationService._

  def listSmsEndpoints(parent: String): ZIO[Any, Throwable, Option[User]] = {
    //    val x = repo.list[SmsEndpoint](parent)

    val opt = Some(
      User(
        id = "SMS1",
        region = "001",
        code = "123",
        parent = "provider#3"
      )
    )

    ZIO.succeed(opt)
  }

}

object ConfigurationService {
  @resource_prefix("user")
  case class User(@id_field id: String,
                  @indexed(indexName = GSI_INDEX_NAME2, pkName = GSI_PK2, skName = GSI_SK2)
                  region: String,
                  @validate(Validation.minLength(3))
                  code: String,
                  @parent_field parent: String) // will keep it as a string for now

  object User {
    implicit val schema: Schema[User] = DeriveSchema.gen
    implicit val jsonEncoder = JsonCodec.jsonCodec[User](schema)
  }

  val mccField: WithFieldName[User, _, String] = User.schema match {
    case Schema.CaseClass4(_, id, mcc, mnc, parent, _, _) =>
      mcc match {
        case f: Field[User, String] => f
      }
  }

  val live: ZLayer[Repository, Nothing, ConfigurationService] = ZLayer.fromFunction(ConfigurationService.apply _)

}
