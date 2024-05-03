package configuration.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import configuration.dao.Dao._
import configuration.dao.untyped.UntypedRepository
import configuration.{CreateTable, DynamoContainer, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.Item
import zio.{Exit, IO, Unsafe, ZLayer}
import scala.concurrent.duration.DurationInt

class UntypedDaoSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

//  "test" in withDynamo { layer =>
//    CreateTable.createTableExample.execute.provide(layer).runUnsafe
//
//    val repo = UntypedRepository(tableName = CreateTable.TableName, Vector.empty)(layer)
//
//    val x = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map.empty,
//      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
//    )
//
//    val saveResponse = repo.save(x).runUnsafe
//    print(saveResponse)
//
//    val fetchResponse: List[Item] = repo.fetchHistory(
//      parentId = Resource.fromValues("provider", "OT12323"),
//      resource = Resource.fromValues("voice_endpoint", "123")).provideLayer(ZLayer.succeed(zio.Scope.default)).runUnsafe
//
//    fetchResponse.length mustBe 1
//  }
//
//  "versions history" in withDynamo { layer =>
//    CreateTable.createTableExample.execute.provide(layer).runUnsafe
//    val repo = UntypedRepository(tableName = CreateTable.TableName, Vector.empty)(layer)
//
//    val initial = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map("value" -> Field.StringField("v1")),
//      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
//    )
//
//    repo.save(initial).runUnsafe
//
//    val updated = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map("value" -> Field.StringField("v2")),
//      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
//    )
//    repo.save(updated).runUnsafe
//
//    val fetchResponse = repo.fetchHistory(
//      parentId = Resource.fromValues("provider", "OT12323"),
//      resource = Resource.fromValues("voice_endpoint", "123")).provideLayer(ZLayer.succeed(zio.Scope.default)).runUnsafe
//
//    fetchResponse.length mustBe 2
//  }

}

