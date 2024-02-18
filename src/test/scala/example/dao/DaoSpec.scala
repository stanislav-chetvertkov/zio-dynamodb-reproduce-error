package example.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import example.SchemaParser.{ProcessedSchemaTyped, id_field, parent_field, resource_prefix}
import example.dao.Dao._
import example.{CreateTable, DynamoContainer, SchemaParser, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.Item
import zio.schema.{DeriveSchema, DynamicValue, Schema}
import zio.{Exit, IO, Unsafe, ZLayer}

import scala.concurrent.duration.DurationInt
import scala.annotation.StaticAnnotation


class DaoSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

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

  // create annotation for marking a field as a partition key

  @resource_prefix("sms_endpoint")
  case class SmsEndpoint(@id_field id: String,
                         value: String,
                         @parent_field parent: String) // will keep it as a string for now

  implicit val schema: Schema[SmsEndpoint] = DeriveSchema.gen

  "write typed schema" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    implicit val writer: ProcessedSchemaTyped[SmsEndpoint] = SchemaParser.validateTyped(schema)

    val repo = Repository(tableName = CreateTable.TableName, Vector.empty)(layer)
    val saveResult = repo.saveTyped(SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")).runUnsafe

    val readResult = repo.readTyped[SmsEndpoint]("SMS1", "provider#3").runUnsafe
    readResult.get mustBe SmsEndpoint("SMS1", "payload", "provider#3")
  }

  "typed schema" in {

    implicit val schema: Schema[SmsEndpoint] = DeriveSchema.gen
    implicit val writer: ProcessedSchemaTyped[SmsEndpoint] = SchemaParser.validateTyped(schema)

    val instance = SmsEndpoint(id = "1", value = "2", parent = "3")
    val r = implicitly[SchemaParser.ProcessedSchemaTyped[SmsEndpoint]].toAttrMap(instance)
    print(r)
  }

  "test schema" in {
    val instance = SmsEndpoint("1", "2", "3")

    val writer = SchemaParser.validate(schema)
    val dynamic = DynamicValue.fromSchemaAndValue(schema, instance)
    writer.toAttrMap(dynamic) mustBe Right(())

    schema.fromDynamic(schema.toDynamic(SmsEndpoint("1", "2", "3"))).toOption mustBe Some(SmsEndpoint("1", "2", "3"))
  }

  "test" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe

    val repo = Repository(tableName = CreateTable.TableName, Vector.empty)(layer)

    val x = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map.empty,
      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
    )

    val saveResponse = repo.save(x).runUnsafe
    print(saveResponse)

    val fetchResponse: List[Item] = repo.fetchHistory(
      parentId = Resource.fromValues("provider", "OT12323"),
      resource = Resource.fromValues("voice_endpoint", "123")).provideLayer(ZLayer.succeed(zio.Scope.default)).runUnsafe

    fetchResponse.length mustBe 1
  }

  "versions history" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName, Vector.empty)(layer)

    val initial = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map("value" -> Field.StringField("v1")),
      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
    )

    repo.save(initial).runUnsafe

    val updated = MappedEntity(ResourceType("voice_endpoint"), ResourceId("123"), Map("value" -> Field.StringField("v2")),
      Some(MappedEntity(ResourceType("provider"), ResourceId("OT12323"), Map.empty, None)),
    )
    repo.save(updated).runUnsafe

    val fetchResponse = repo.fetchHistory(
      parentId = Resource.fromValues("provider", "OT12323"),
      resource = Resource.fromValues("voice_endpoint", "123")).provideLayer(ZLayer.succeed(zio.Scope.default)).runUnsafe

    fetchResponse.length mustBe 2
  }

}
