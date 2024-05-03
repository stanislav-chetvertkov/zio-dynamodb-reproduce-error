package configuration

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.endpoints.Endpoint
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider

import java.net.URI
import java.util.concurrent.CompletableFuture

object DynamoDbLocalContainer {
  private val DEFAULT_IMAGE_NAME = DockerImageName.parse("amazon/dynamodb-local")
  private val MAPPED_PORT = 8000

  def apply(dockerImageName: String): DynamoDbLocalContainer = {
    new DynamoDbLocalContainer(DockerImageName.parse(dockerImageName))
  }
}

class DynamoDbLocalContainer(dockerImageName: DockerImageName) extends GenericContainer[DynamoDbLocalContainer](dockerImageName) {
  dockerImageName.assertCompatibleWith(DynamoDbLocalContainer.DEFAULT_IMAGE_NAME)
  withExposedPorts(DynamoDbLocalContainer.MAPPED_PORT)


  def getClient: DynamoDbClient =
    DynamoDbClient.builder()
      .credentialsProvider(getCredentials)
      .endpointProvider(getEndpointConfiguration)
      .region(Region.US_EAST_1)
      .build();

  def urlValue = Endpoint.builder()
    .url(URI.create(s"http://${getHost}:${getMappedPort(DynamoDbLocalContainer.MAPPED_PORT)}")).build()

  def getEndpointConfiguration: DynamoDbEndpointProvider = (_: DynamoDbEndpointParams) => {
    CompletableFuture.completedFuture(urlValue)
  }

  def getCredentials: AwsCredentialsProvider = () => new AwsCredentials {
    override def accessKeyId(): String = "dummy"

    override def secretAccessKey(): String = "dummy"
  }
}