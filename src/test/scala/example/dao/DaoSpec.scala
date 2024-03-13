package example.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import example.SchemaParser.{ProcessedSchemaTyped, id_field, indexed, parent_field, resource_prefix}
import example.dao.Dao._
import example.dao.untyped.UntypedRepository
import example.{CreateTable, DynamoContainer, SchemaParser, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.{Item, ProjectionExpression}
import zio.schema.{DeriveSchema, DynamicValue, Schema}
import zio.{Exit, IO, Unsafe, ZLayer}

import java.time.Instant
import scala.concurrent.duration.DurationInt

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

  @resource_prefix("sms_endpoint")
  case class SmsEndpoint(@id_field id: String,
                         value: String,
                         @parent_field parent: String) // will keep it as a string for now

  @resource_prefix("voice_endpoint")
  case class VoiceEndpoint(@id_field voice_id: String,
                           ip: String,
                           capacity: Int,
                           @parent_field parent: String) // will keep it as a string for now

  @resource_prefix("dlr")
  case class DlrErrorCodeMapping(@id_field id: String,
                                 @indexed("gsi2") mcc: String,
                                 errorCode: String,
                                 @parent_field parent: String)

  object DlrErrorCodeMapping {

    implicit val dlrSchema: Schema.CaseClass4[String, String, String, String, DlrErrorCodeMapping] = DeriveSchema.gen[DlrErrorCodeMapping]
    val (id, mcc, errorCode, parent) = ProjectionExpression.accessors[DlrErrorCodeMapping]
  }

  implicit val smsSchema: Schema[SmsEndpoint] = DeriveSchema.gen
  implicit val voiceSchema: Schema[VoiceEndpoint] = DeriveSchema.gen


  //  "query resource by field"  in withDynamo { layer =>
  //    CreateTable.createTableExample.execute.provide(layer).runUnsafe
  //    val repo = Repository(tableName = CreateTable.TableName)(layer)
  //
  //    val mcc: ProjectionExpression[DlrErrorCodeMapping, String] = DlrErrorCodeMapping.mcc
  //    mcc.elementAt()
  //  }

  "optimistic locking" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val voiceEndpointV2 = voiceEndpoint.copy(capacity = 20)

    val resp = repo.save(voiceEndpoint).runUnsafe

    repo.save(voiceEndpoint.copy(capacity = 20), currentVersion = Some(1)).runUnsafe

    repo.readHistory[VoiceEndpoint]("voice1").runUnsafe mustBe Some(voiceEndpoint)
  }


  "optimistic locking on writes" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val voiceEndpoint2 = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 20, parent = "provider#3")

    repo.update(voiceEndpoint).runUnsafe
    val hist1 = repo.readHistory[VoiceEndpoint]("voice1").runUnsafe

    hist1.length mustBe 1
    hist1.head._2 mustBe 1

    val saveWithoutClashingVersion = repo.update(voiceEndpoint2, 1).runUnsafe
    //      saveWithoutClashingVersion must not be(empty)

    val x2 = repo.readHistory[VoiceEndpoint]("voice1").runUnsafe
    x2.length mustBe 2

    //      val saveWithUniqueVersion = repo.saveTypedOptimisticLoc(voiceEndpoint2, 2).runUnsafe
    //      saveWithUniqueVersion must not be empty

    //      repo.readHistory[VoiceEndpoint]("voice1").runUnsafe.length mustBe 2
  }

  "get resource history by id (without parent)" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    repo.update(smsEndpoint1).runUnsafe

    val smsEndpoint1_updated = SmsEndpoint(id = "SMS1", value = "updated payload", parent = "provider#3")
    repo.update(smsEndpoint1_updated).runUnsafe

    val history = repo.readHistory[SmsEndpoint](id = "SMS1").runUnsafe
    history.length mustBe 2
    history.map(_._1) mustBe List(smsEndpoint1, smsEndpoint1_updated) //fields are in the wrong order

    val timestamps = history.map(_._2)
    //    Instant.parse(timestamps(0)).isBefore(Instant.parse(timestamps(1))) mustBe true //fixme reenable
  }

  "get resource by id (without specifying its parent)" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    repo.update(smsEndpoint1).runUnsafe

    val opt = repo.read[SmsEndpoint]("SMS1").runUnsafe
    opt.get mustBe SmsEndpoint(id = "SMS1", value = "payload", "provider#3")
  }

  "list all resource by type" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")
    val smsEndpoint3 = SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")

    repo.update(smsEndpoint1).runUnsafe
    repo.update(smsEndpoint2).runUnsafe
    repo.update(smsEndpoint3).runUnsafe

    val list = repo.listAll[SmsEndpoint].runUnsafe
    list.toSet mustBe Set(smsEndpoint1, smsEndpoint2, smsEndpoint3)
  }

  "scan all resource" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")
    val smsEndpoint3 = SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")

    repo.update(smsEndpoint1).runUnsafe
    repo.update(smsEndpoint2).runUnsafe
    repo.update(smsEndpoint3).runUnsafe

    val list = repo.scanAll[SmsEndpoint].runUnsafe
    list.toSet mustBe Set(smsEndpoint1, smsEndpoint2, smsEndpoint3)
  }

  "write typed schema (voice)" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val saveResult = repo.update(voiceEndpoint).runUnsafe

    val opt = repo.read[VoiceEndpoint]("voice1").runUnsafe
    opt.get mustBe voiceEndpoint
  }

  "write typed schema" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    implicit val schemaProcessor: ProcessedSchemaTyped[SmsEndpoint] = SchemaParser.validateTyped(smsSchema)

    val repo = Repository(tableName = CreateTable.TableName)(layer)

    val saveResult = repo.update(SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")).runUnsafe

    val readResult = repo.readTypedByParent[SmsEndpoint]("SMS1", "provider#3").runUnsafe
    readResult.get mustBe SmsEndpoint("SMS1", "payload", "provider#3")

    repo.update(SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")).runUnsafe
    repo.update(SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")).runUnsafe

    val list = repo.list[SmsEndpoint]("provider#3").runUnsafe
    list.length mustBe 2

    val list2 = repo.list[SmsEndpoint]("provider#4").runUnsafe
    list2.length mustBe 1
  }

  "typed schema" in {

    implicit val schema: Schema[SmsEndpoint] = DeriveSchema.gen
    implicit val writer: ProcessedSchemaTyped[SmsEndpoint] = SchemaParser.validateTyped(schema)

    val instance = SmsEndpoint(id = "1", value = "2", parent = "3")
    val r = implicitly[SchemaParser.ProcessedSchemaTyped[SmsEndpoint]].toAttrMap(instance)
    r.map.get("sk").map(_.decode[String].value).get must startWith("sms_endpoint#1")

    print(r)
  }

  "test schema" in {
    val instance = SmsEndpoint("1", "2", "3")

    val writer = SchemaParser.validate(smsSchema)
    val dynamic = DynamicValue.fromSchemaAndValue(smsSchema, instance)
    writer.toAttrMap(dynamic) mustBe Right(())

    smsSchema.fromDynamic(smsSchema.toDynamic(SmsEndpoint("1", "2", "3"))).toOption mustBe Some(SmsEndpoint("1", "2", "3"))
  }

  "test" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe

    val repo = UntypedRepository(tableName = CreateTable.TableName, Vector.empty)(layer)

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
    val repo = UntypedRepository(tableName = CreateTable.TableName, Vector.empty)(layer)

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
