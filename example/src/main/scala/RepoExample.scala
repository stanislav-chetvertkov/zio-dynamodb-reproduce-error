import example.CreateTable
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio._
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor
import zio.http._
import zio.http.codec.HttpCodec.query
import zio.http.endpoint.Endpoint

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

    //disable if the table is already created
    CreateTable.createTableExample.execute.provide(executorLayer)

    executorLayer.orDie
  }

  val getUser =
    Endpoint(Method.GET / "users" / int("userId")).out[Int]

  val getUserRoute =
    getUser.implement {
      Handler.fromFunction[Int] { id =>
        id
      }
    }

  val getUserPosts =
    Endpoint(Method.GET / "users" / int("userId") / "posts" / int("postId"))
      .query(query("name"))
      .out[List[String]]

  val r = Method.GET / "text" -> handler(Response.text("Hello World!"))

  val routes = Routes(getUserRoute, r)

  val app = routes.toHttpApp

  override val run =
    Server.serve(app).provide(Server.default)
}

trait ApiHandler {
  def getEndpoint(parent: Int): Response
}