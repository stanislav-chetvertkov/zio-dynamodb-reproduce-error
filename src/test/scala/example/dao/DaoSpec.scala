package example.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import example.dao.Dao.MappedEntity
import example.{CreateTable, DynamoContainer, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import zio.{Exit, IO, Unsafe, ZIO, ZLayer}
import zio.aws.core.config.AwsConfig
import zio.aws.dynamodb.DynamoDb
import zio.aws.netty.NettyHttpClient
import zio.dynamodb.DynamoDBExecutor
import Dao._
import zio.Runtime.default

import java.net.URI
import scala.concurrent.duration.DurationInt

class DaoSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

  // run unsafe for instance of for {
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

  "test" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe

    val repo = Repository(tableName = CreateTable.TableName, Vector.empty)(layer)

    val x = MappedEntity(ResourcePrefix("voice_endpoint"), "123", Map.empty,
      Some(MappedEntity(ResourcePrefix("provider"), "OT12323", Map.empty, None)),
    )

    val saveResponse = repo.save(x).runUnsafe
    print(saveResponse)

    val fetchResponse = repo.fetch(
      parentId = Resource.fromValues("provider", "OT12323"),
      resource = Resource.fromValues("voice_endpoint", "123")).provideLayer(ZLayer.succeed(zio.Scope.default)).runUnsafe
    print(fetchResponse)
  }

}
