package configuration.dao.untyped

import configuration.dao.untyped.UntypedDao.{IdMapping, MappedEntity, Resource}
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.*
import zio.{Chunk, ULayer, ZIO}

case class UntypedRepository(tableName: String,
                      resourceMappings: Vector[IdMapping])
                     (executor: ULayer[DynamoDBExecutor]) {

  def save(entity: MappedEntity): ZIO[Any, Throwable, Option[Item]] = {
    val attrMap: AttrMap = toAttrMap(entity, System.currentTimeMillis())
    DynamoDBQuery.putItem(tableName, attrMap).execute.provideLayer(executor)
  }

  def fetchHistory(parentId: Resource, resource: Resource): ZIO[Any, Throwable, List[Item]] = {
    val pkValue = parentId.flatPrefix
    val sortPrefix = resource.flatPrefix
    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === pkValue && $("sk").sortKey.beginsWith(sortPrefix))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.toList)
  }

  def fetch(parentId: Resource, resource: Resource): ZIO[Any, Throwable, Option[Item]] = {
    val pkValue = parentId.flatPrefix
    val sortPrefix = resource.flatPrefix
    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === pkValue && $("sk").sortKey === sortPrefix)

    query.execute.provideLayer(executor).map(_._1.headOption)
  }

  private def toAttrMap(entity: MappedEntity, millis: Long) = {
    val attributes : Map[String, AttributeValue] = Map(
      "pk" -> AttributeValue(entity.parent.map(_.resourcePrefix.value).getOrElse("no_resource_type") + "#" + entity.parent.map(_.id.value).getOrElse("missing_id")),
      "sk" -> AttributeValue(entity.sk(millis)), // "voice_endpoint#123#123123123123",
      "resourcePrefix" -> AttributeValue(entity.resourcePrefix.value),
      "id" -> AttributeValue(entity.id.value)
    )

    val k = entity.fields.toList.map(f => f._1 -> f._2.toAttributeValue).toMap

    val x = AttrMap(attributes ++ k)
    x
  }

}

object UntypedDao {
  // when a new entity is being persisted there could be no id yet, there has to be a function that gives us the id
  // based on the resource prefix, we should be able to get this information after parsing the specs

  type IdMapping = ResourceType => Option[MappedEntity] => ResourceId

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

  case class ResourceType(value: String) extends AnyVal

  case class ResourceId(value: String) extends AnyVal

  // given resource type and an object(entity) of this type we could generate a resource id (or extract it from the entity if it could be derived from the entity itself

  case class MappedEntity(resourcePrefix: ResourceType, id: ResourceId, fields: Map[String, Field], parent: Option[MappedEntity]) {
    def sk(time: Long) = s"${resourcePrefix.value}#${id.value}#$time"
  }

  case class Resource(resourceType: ResourceType, id: ResourceId) {
    def flatPrefix = s"${resourceType.value}#${id.value}"
  }

  object Resource {
    def fromValues(prefix: String, id: String): Resource = Resource(ResourceType(prefix), ResourceId(id))
  }
}
