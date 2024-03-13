package example.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import example.SchemaParser.{ProcessedSchemaTyped, id_field, parent_field, resource_prefix}
import example.{CreateTable, DynamoContainer, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.AttrMap
import zio.dynamodb.SchemaUtils.{Timestamp, Version}
import zio.schema.{DeriveSchema, Schema}
import zio.{Exit, IO, Unsafe}
import scala.concurrent.duration.DurationInt

class OptimisticLockingSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

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
  @resource_prefix("voice_endpoint")
  case class VoiceEndpoint(@id_field voice_id: String,
                           ip: String,
                           capacity: Int,
                           @parent_field parent: String) // will keep it as a string for now

  implicit val voiceSchema: Schema[VoiceEndpoint] = DeriveSchema.gen

  "schema serialize with history=false" in {
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")

    val processor = implicitly[ProcessedSchemaTyped[VoiceEndpoint]]

    val props = processor.toAttrMap(voiceEndpoint, 1, isHistory = false)
    println(props)

    processor.fromAttrMap(props) mustBe voiceEndpoint
  }

  "schema serialize with history=true" in {
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val processor = implicitly[ProcessedSchemaTyped[VoiceEndpoint]]

    val props: AttrMap = processor.toAttrMap(voiceEndpoint, 1, isHistory = true)
    props.get[String]("gsi_sk1") mustBe Right("history#voice1#1")
    processor.fromAttrMap(props) mustBe voiceEndpoint
  }

  "write should succeed when the version is consistent" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val voiceEndpointV2 = voiceEndpoint.copy(capacity = 20)
    val voiceEndpointV3 = voiceEndpoint.copy(capacity = 30)

    repo.save(voiceEndpoint).runUnsafe

    repo.save(voiceEndpointV2, currentVersion = Some(1)).runUnsafe
    repo.save(voiceEndpointV3, currentVersion = Some(2)).runUnsafe

    val history: List[(VoiceEndpoint, Version, Timestamp)] = repo.readHistory[VoiceEndpoint]("voice1").runUnsafe
    history.length mustBe 3

    val first = history.head
    val second = history(1)
    val third = history(2)
    first._1 mustBe voiceEndpoint
    first._2 mustBe 1

    second._1 mustBe voiceEndpointV2
    second._2 mustBe 2

    third._1 mustBe voiceEndpointV3
    third._2 mustBe 3

    repo.read[VoiceEndpoint]("voice1").runUnsafe mustBe Some(voiceEndpointV3)
  }

  "should fail when the version is inconsistent" in withDynamo { layer =>
    CreateTable.createTableExample.execute.provide(layer).runUnsafe
    val repo = Repository(tableName = CreateTable.TableName)(layer)
    val voiceEndpoint = VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
    val voiceEndpointV2 = voiceEndpoint.copy(capacity = 20)
    val voiceEndpointV3 = voiceEndpoint.copy(capacity = 30)

    repo.save(voiceEndpoint).runUnsafe
    repo.save(voiceEndpointV2, currentVersion = Some(1)).runUnsafe
    repo.save(voiceEndpointV3, currentVersion = Some(1)).runUnsafe mustBe None // version should be 2 - this request will be rejected

    val history: List[(VoiceEndpoint, Version, Timestamp)] = repo.readHistory[VoiceEndpoint]("voice1").runUnsafe
    history.length mustBe 2

    repo.read[VoiceEndpoint]("voice1").runUnsafe mustBe Some(voiceEndpointV2)
  }


}
