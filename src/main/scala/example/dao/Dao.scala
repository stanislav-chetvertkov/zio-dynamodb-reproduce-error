package example.dao

import example.dao.Dao.{IdMapping, MappedEntity, Resource, ResourceId, ResourcePrefix}
import zio.dynamodb.ProjectionExpression.$
import zio.{Chunk, ULayer, ZIO, stream}
import zio.dynamodb.{AttrMap, AttributeValue, ConditionExpression, DynamoDBExecutor, DynamoDBQuery, Item, KeyConditionExpr, LastEvaluatedKey}



object Dao {
  type IdMapping = ResourcePrefix => Option[MappedEntity] => ResourceId

  sealed trait Field {
    def toAttributeValue: AttributeValue
  }
  object Field {
    case class StringField(value: String) extends Field {
      override def toAttributeValue: AttributeValue = AttributeValue(value)
    }
    case class IntField(value: Int) extends Field {
      override def toAttributeValue: AttributeValue = AttributeValue(value)
    }
    case class BooleanField(value: Boolean) extends Field {
      override def toAttributeValue: AttributeValue = AttributeValue(value)
    }
  }

  case class Resource(prefix: ResourcePrefix, id: ResourceId)

  object Resource {
    def fromValues(prefix: String, id: String): Resource = Resource(ResourcePrefix(prefix), ResourceId(id))
  }

  case class ResourcePrefix(value: String) extends AnyVal
  case class ResourceId(value: String) extends AnyVal

  // given resource type and an object(entity) of this type we could generate a resource id (or extract it from the entity if it could be derived from the entity itself


  case class MappedEntity(resourcePrefix: ResourcePrefix, id: String, fields: Map[String, Field], parent: Option[MappedEntity]) {
    def sk(time: Long) = s"${resourcePrefix.value}#$id#$time"
  }

  // when a new entity is being persisted there could be no id yet, there has to be a function that gives us the id
  // based on the resource prefix, we should be able to get this information after parsing the specs

}

case class Repository(tableName: String,
                      resourceMappings: Vector[IdMapping])
                     (executor: ULayer[DynamoDBExecutor]) {

  def save(entity: MappedEntity): ZIO[Any, Throwable, Option[Item]] = {
    val attrMap = toAttrMap(entity)
    DynamoDBQuery.putItem(tableName, attrMap).execute.provideLayer(executor)
  }

  def fetch(parentId: Resource, resource: Resource): ZIO[Any, Throwable, List[Item]] = {
    val pkValue = parentId.prefix.value + "#" + parentId.id.value
    val sortPrefix = resource.prefix.value + "#" + resource.id.value
    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === pkValue && $("sk").sortKey.beginsWith(sortPrefix))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.toList)
  }

  def toAttrMap(entity: MappedEntity) = {
    val attributes : Map[String, AttributeValue] = Map(
      "pk" -> AttributeValue(entity.parent.map(_.resourcePrefix.value).getOrElse("no_resource_type") + "#" + entity.parent.map(_.id).getOrElse("missing_id")),
      "sk" -> AttributeValue(entity.sk(System.currentTimeMillis())), // "voice_endpoint#123#123123123123",
      "resourcePrefix" -> AttributeValue(entity.resourcePrefix.value),
      "id" -> AttributeValue(entity.id)
    )

    val k = entity.fields.toList.map(f => f._1 -> f._2.toAttributeValue).toMap

    val x = AttrMap(attributes ++ k)
    x
  }

}
