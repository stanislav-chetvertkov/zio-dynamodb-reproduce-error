import api.generic.GenericHandlerImpl.Processors
import api.generic.{GenericHandler, GenericHandlerImpl, GenericResource}
import api.{StoreHandler, StoreHandlerImpl, StoreResource}
import configuration.{CreateTable, SchemaParser}
import configuration.SchemaParser.{id_field, parent_field, resource_prefix}
import configuration.dao.Repository
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.*
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor
import zio.http.*
import zio.schema.{DeriveSchema, Schema}

import java.net.URI

object RepoExample extends ZIOAppDefault {

  @resource_prefix("sms_endpoint")
  case class SmsEndpoint(@id_field id: String,
                         value: String,
                         @parent_field parent: String) // will keep it as a string for now

  @resource_prefix("voice_endpoint")
  case class VoiceEndpoint(@id_field voice_id: String,
                           ip: String,
                           capacity: Int,
                           @parent_field parent: String) // will keep it as a string for now

  implicit val smsSchema: Schema[SmsEndpoint] = DeriveSchema.gen
  implicit val voiceSchema: Schema[VoiceEndpoint] = DeriveSchema.gen
  
  val processors: Processors = Processors(
    Map(
      ("sms_endpoint", SchemaParser.validate(smsSchema)),
      ("voice_endpoint", SchemaParser.validate(voiceSchema))
    )
  )
  

  def createExecutorForDockerCompose: ZLayer[Any, Nothing, DynamoDBExecutor] = {
    val dynLayer = DynamoDb.customized(
      _.region(Region.US_EAST_1)
        .endpointOverride(new URI("http://localhost:8000"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
    )

    val dynamoClientLayer = NettyHttpClient.default >>> AwsConfig.default >>> dynLayer
    val executorLayer = dynamoClientLayer >>> DynamoDBExecutor.live

    executorLayer.orDie
  }

  override val run = {
    val program: ZIO[GenericHandler with StoreHandler with DynamoDBExecutor, Throwable, Unit] = for {
      _ <- CreateTable.deleteTableQuery.execute.ignore
      _ <- CreateTable.createTableExample.execute
      apiHandler <- ZIO.service[StoreHandler]
      genericHandler <- ZIO.service[GenericHandler]
      httpApp: HttpApp[Any] = {
        StoreResource.routes(apiHandler).toHttpApp ++ GenericResource.routes(genericHandler).toHttpApp
      }
      _ <- Server.serve(httpApp).provide(Server.defaultWithPort(8082))
    } yield ()

    program.provide(
      StoreHandlerImpl.live,
      createExecutorForDockerCompose,
      Repository.live,
      ZLayer.succeed(Repository.Config(CreateTable.TableName)),
      GenericHandlerImpl.live,
      ZLayer.succeed(processors)
    )
  }
}
