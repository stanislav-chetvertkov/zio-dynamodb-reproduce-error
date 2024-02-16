package example

import com.dimafeng.testcontainers.scalatest.{IllegalWithContainersCall, TestContainerForEach}
import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import example.DynamoContainer.defaultDockerImageName
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.endpoints.Endpoint
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import zio.{ULayer, ZLayer}
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

  def withDynamo[A](runTest: ULayer[DynamoDBExecutor] => A): A = {
    withContainers{dyna =>
      val executorLayer = createLayer(dyna.urlValue.url())
      runTest(executorLayer)
    }
  }

}