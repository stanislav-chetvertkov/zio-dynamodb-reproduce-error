package configuration

import com.dimafeng.testcontainers.scalatest.{IllegalWithContainersCall, TestContainerForEach}
import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import configuration.DynamoContainer.defaultDockerImageName
import configuration.dao.Repository
import configuration.dao.Repository.Config
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.endpoints.Endpoint
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.{Exit, IO, ULayer, Unsafe, ZLayer}
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor

import java.net.URI


final case class DynamoContainer(dockerImageName: DockerImageName = DockerImageName.parse(defaultDockerImageName))
  extends SingleContainer[DynamoDbLocalContainer] {

  override val container: DynamoDbLocalContainer = {
    new DynamoDbLocalContainer(dockerImageName)
  }

  def client: DynamoDbClient = container.getClient

  def urlValue: Endpoint = container.urlValue

}

object DynamoContainer {

  val defaultDockerImageName = "amazon/dynamodb-local"

  final case class Def(dockerImageName: DockerImageName = DockerImageName.parse(defaultDockerImageName)) extends ContainerDef {

    override type Container = DynamoContainer

    override def createContainer(): DynamoContainer = DynamoContainer(dockerImageName)
  }
}


trait WithDynamoDB {
  self: TestContainerForEach =>

  implicit class UnsafeOps[E, A](action: IO[E, A]) {
    def runUnsafe: A = {
      Unsafe.unsafe { implicit unsafe =>
        zio.Runtime.default.unsafe.run(action) match {
          case Exit.Success(value) => value
          case Exit.Failure(cause) => throw new RuntimeException(cause.prettyPrint)
        }
      }
    }
  }

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  def createLayer(url: URI): ZLayer[Any, Nothing, DynamoDBExecutor] = {
    val dynLayer = DynamoDb.customized(
      _.region(Region.US_EAST_1)
        .endpointOverride(url)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
    )

    val dynamoClientLayer = NettyHttpClient.default >>> AwsConfig.default >>> dynLayer
    val executorLayer = dynamoClientLayer >>> DynamoDBExecutor.live
    executorLayer.orDie
  }

  def createLayer2(url: URI): DynamoDBExecutor = {
    val dynLayer = DynamoDb.customized(
      _.region(Region.US_EAST_1)
        .endpointOverride(url)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
    )

    val dynamoClientLayer = NettyHttpClient.default >>> AwsConfig.default >>> dynLayer
    val executorLayer = dynamoClientLayer >>> DynamoDBExecutor.live
    executorLayer.orDie.build(zio.Scope.global).runUnsafe.get
  }

  def withDynamoDao[A](runTest: Repository => A): A = {
    withContainers { dyna =>
      val executorLayer = createLayer2(dyna.urlValue.url())
      CreateTable.createTableExample.execute.provide(ZLayer.succeed(executorLayer)).runUnsafe
      val dao: Repository = Repository(
        Config(CreateTable.TableName),
        executorLayer,
        10
      )

      runTest(dao)
    }
  }


  def withDynamo[A](runTest: ULayer[DynamoDBExecutor] => A): A = {
    withContainers { dyna =>
      val executorLayer = createLayer(dyna.urlValue.url())
      println(dyna.portBindings)
      runTest(executorLayer)
    }
  }

}