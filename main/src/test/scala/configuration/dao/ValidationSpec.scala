package configuration.dao

import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import configuration.ConfigSchemaCodec.{IndexName, id_field, indexed, parent_field, resource_prefix}
import configuration.TableStructure.*
import configuration.{ConfigSchemaCodec, DynamoContainer, WithDynamoDB}
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.must.Matchers
import zio.dynamodb.ProjectionExpression
import zio.schema.Schema.Field
import zio.schema.Schema.Field.WithFieldName
import zio.schema.annotation.validate
import zio.schema.validation.Validation
import zio.schema.{DeriveSchema, Schema}

import scala.concurrent.duration.DurationInt

class ValidationSpec extends AnyFreeSpecLike with ScalaFutures with Matchers with EitherValues with TestContainerForEach with WithDynamoDB {

  override val containerDef: DynamoContainer.Def = DynamoContainer.Def()

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(1.seconds, 50.millis)

  @resource_prefix("sms_endpoint")
  case class SmsEndpoint(@id_field id: String,
                         @indexed(indexName = IndexName[String](GSI_INDEX_NAME2), pkName = GSI_PK2, skName = GSI_SK2) //todo: needs to fail compilation if the macro type does not align
                         mcc: String,
                         @validate(Validation.maxLength(3))
                         mnc: String,
                         @parent_field parent: String) // will keep it as a string for now

  implicit val smsSchema: Schema[SmsEndpoint] = DeriveSchema.gen


  @resource_prefix("user")
  case class User(@id_field id: String,
                  @validate(Validation.minLength(3))
                  name: String,
                  ids: List[Long],
                  @parent_field parent: String) // will keep it as a string for now

  implicit val userSchema: Schema[User] = DeriveSchema.gen

  case class Address(zipCode: String, street: String, city: String)

  @resource_prefix("nested_user")
  case class NestedUser(@id_field id: String,
                        @validate(Validation.minLength(3))
                        name: String,
                        address: Address,
                        @parent_field parent: String) // will keep it as a string for now

  implicit val nestedUserSchema: Schema[NestedUser] = DeriveSchema.gen


  val mccField: WithFieldName[SmsEndpoint, ?, String] = smsSchema match {
    case Schema.CaseClass4(_, id, mcc, mnc, parent, _, _) =>
      mcc match {
        case f: Field[SmsEndpoint, String] => f
        case _ => throw new RuntimeException("mcc field not found")
      }
  }

  object SmsEndpoint {
    implicit val smsSchema: Schema.CaseClass4[String, String, String, String, SmsEndpoint] = DeriveSchema.gen[SmsEndpoint]
    val (id, mcc, mnc, parent) = ProjectionExpression.accessors[SmsEndpoint]
    mnc.partitionKey
  }

  "nested user test" in {
    val processor = ConfigSchemaCodec.fromSchema(nestedUserSchema)
    val user = NestedUser("1", "name" , Address("12345", "street", "city"), "parent#1")

    val attrs = processor.toAttrMap(user)
    println(attrs)
    processor.fromAttrMap(attrs) mustBe user
  }


  "test" in {
    val processor = ConfigSchemaCodec.fromSchema(userSchema)
    val user = User("1", "name", List(1l, 2l, 3l), "parent#1")

    val attrs = processor.toAttrMap(user)
    println(attrs)
    processor.fromAttrMap(attrs) mustBe user
  }

  "query by field" in withDynamoDao { repo =>

    val smsEndpoint1 = SmsEndpoint(id = "SMS1", mcc = "001", mnc = "123", parent = "provider#3")
    val smsEndpoint2 = SmsEndpoint(id = "SMS2", mcc = "002", mnc = "123", parent = "provider#4")
    val smsEndpoint3 = SmsEndpoint(id = "SMS5", mcc = "001", mnc = "123", parent = "provider#5")

    // insert
    repo.save(smsEndpoint1).runUnsafe
    repo.save(smsEndpoint2).runUnsafe
    repo.save(smsEndpoint3).runUnsafe

    val result: List[SmsEndpoint] = repo.queryByParameter(mccField, "001").runUnsafe

    result must contain theSameElementsAs List(smsEndpoint1, smsEndpoint3)
  }

}
