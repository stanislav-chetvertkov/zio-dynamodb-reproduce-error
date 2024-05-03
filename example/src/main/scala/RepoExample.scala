import api.{StoreHandler, StoreHandlerImpl, StoreResource}
import configuration.CreateTable
import configuration.dao.Repository
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor
import zio.http._

import java.net.URI

object RepoExample extends ZIOAppDefault {

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
    val program: ZIO[StoreHandler with DynamoDBExecutor, Throwable, Unit] = for {
      _ <- CreateTable.deleteTableQuery.execute.ignore
      _ <- CreateTable.createTableExample.execute
      apiHandler <- ZIO.service[StoreHandler]
      httpApp = StoreResource.routes(apiHandler).toHttpApp
      _ <- Server.serve(httpApp).provide(Server.defaultWithPort(8082))
    } yield ()

    program.provide(
      StoreHandlerImpl.live,
      createExecutorForDockerCompose,
      Repository.live,
      ZLayer.succeed(Repository.Config(CreateTable.TableName))
    )
  }
}
