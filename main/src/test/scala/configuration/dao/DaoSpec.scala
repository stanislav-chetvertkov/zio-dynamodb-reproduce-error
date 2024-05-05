package configuration.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import configuration.ConfigSchemaCodec.{id_field, indexed, parent_field, resource_prefix}
import configuration.{ConfigSchemaCodec, CreateTable, DynamoContainer, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.ProjectionExpression
import zio.schema.{DeriveSchema, Schema}
import zio.{Exit, IO, Unsafe}

import scala.concurrent.duration.DurationInt

class DaoSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

  case class Network(mcc: String, mnc: String)

  @resource_prefix("sms_endpoint_ext")
  case class SmsEndpointExt(@id_field id: String,
                            network: Network,
                            @parent_field parent: String)

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
                                 mcc: String,
                                 errorCode: String,
                                 @parent_field parent: String)

  object DlrErrorCodeMapping {

    implicit val dlrSchema: Schema.CaseClass4[String, String, String, String, DlrErrorCodeMapping] = DeriveSchema.gen[DlrErrorCodeMapping]
    val (id, mcc, errorCode, parent) = ProjectionExpression.accessors[DlrErrorCodeMapping]
  }

  implicit val smsSchemaExt: Schema[SmsEndpointExt] = DeriveSchema.gen
  implicit val smsSchema: Schema[SmsEndpoint] = DeriveSchema.gen
  implicit val voiceSchema: Schema[VoiceEndpoint] = DeriveSchema.gen

  "what if I save the same record twice" in withDynamoDao { repo =>
    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS1", value = "payload2", parent = "provider#3")
    val w1 = repo.save(smsEndpoint1).runUnsafe
    val w2 = repo.save(smsEndpoint2).runUnsafe

    println(w1)
    println(w2)

    val writes = repo.list[SmsEndpoint]("provider#3").runUnsafe
    println(writes)
    writes.length mustBe 1
    writes.head mustBe smsEndpoint1
  }


  "get resource by id (without specifying its parent)" in withDynamoDao { repo =>

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    repo.save(smsEndpoint1).runUnsafe

    val opt = repo.read[SmsEndpoint]("SMS1").runUnsafe
    opt.get mustBe SmsEndpoint(id = "SMS1", value = "payload", "provider#3")
  }

  "list all resource by type" in withDynamoDao { repo =>
    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")
    val smsEndpoint3 = SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")

    repo.save(smsEndpoint1).runUnsafe
    repo.save(smsEndpoint2).runUnsafe
    repo.save(smsEndpoint3).runUnsafe

    val list = repo.listAll[SmsEndpoint].runUnsafe
    list.toSet mustBe Set(smsEndpoint1, smsEndpoint2, smsEndpoint3)
  }

  "list all resource" in withDynamoDao { repo =>
    val smsEndpoint1 = SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")
    val smsEndpoint3 = SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")

    repo.save(smsEndpoint1).runUnsafe
    repo.save(smsEndpoint2).runUnsafe
    repo.save(smsEndpoint3).runUnsafe

    val list = repo.listAll[SmsEndpoint].runUnsafe
    list.toSet mustBe Set(smsEndpoint1, smsEndpoint2, smsEndpoint3)
  }

  "write typed schema (voice)" in withDynamoDao { repo =>
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val saveResult = repo.save(voiceEndpoint).runUnsafe

    val opt = repo.read[VoiceEndpoint]("voice1").runUnsafe
    opt.get mustBe voiceEndpoint
  }

  "write typed schema" in withDynamoDao { repo =>
    implicit val schemaProcessor: ConfigSchemaCodec[SmsEndpoint] = ConfigSchemaCodec.fromSchema(smsSchema)
    val saveResult = repo.save(SmsEndpoint(id = "SMS1", value = "payload", parent = "provider#3")).runUnsafe

    val readResult = repo.readTypedByParent[SmsEndpoint]("SMS1", "provider#3").runUnsafe
    readResult.get mustBe SmsEndpoint("SMS1", "payload", "provider#3")

    repo.save(SmsEndpoint(id = "SMS2", value = "payload2", parent = "provider#3")).runUnsafe
    repo.save(SmsEndpoint(id = "SMS3", value = "payload3", parent = "provider#4")).runUnsafe

    val list = repo.list[SmsEndpoint]("provider#3").runUnsafe
    list.length mustBe 2

    val list2 = repo.list[SmsEndpoint]("provider#4").runUnsafe
    list2.length mustBe 1
  }

  "write typed schema (sms endpoint ext)" in withDynamoDao { repo =>
    implicit val schemaProcessor: ConfigSchemaCodec[SmsEndpointExt] = ConfigSchemaCodec.fromSchema(DeriveSchema.gen)
    val saveResult = repo.save(SmsEndpointExt("SMS1", Network("mcc", "mnc"), "provider#3")).runUnsafe

    val readResult = repo.readTypedByParent[SmsEndpointExt]("SMS1", "provider#3").runUnsafe
    readResult.get mustBe SmsEndpointExt("SMS1", Network("mcc", "mnc"), "provider#3")

    repo.save(SmsEndpointExt("SMS2", Network("mcc", "mnc"), "provider#3")).runUnsafe
    repo.save(SmsEndpointExt("SMS3", Network("mcc", "mnc"), "provider#4")).runUnsafe

    val list = repo.list[SmsEndpointExt]("provider#3").runUnsafe
    list.length mustBe 2

    val list2 = repo.list[SmsEndpointExt]("provider#4").runUnsafe
    list2.length mustBe 1
  }

  "typed schema" in {

    implicit val schema: Schema[SmsEndpoint] = DeriveSchema.gen
    implicit val writer: ConfigSchemaCodec[SmsEndpoint] = ConfigSchemaCodec.fromSchema(schema)

    val instance = SmsEndpoint(id = "1", value = "2", parent = "3")
    val r = implicitly[ConfigSchemaCodec[SmsEndpoint]].toAttrMap(instance)
    r.map.get("sk").map(_.decode[String].value).get must startWith("sms_endpoint#1")

    print(r)
  }

}
