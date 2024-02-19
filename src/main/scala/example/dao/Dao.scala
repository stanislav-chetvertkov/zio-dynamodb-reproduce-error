package example.dao

import example.SchemaParser.ProcessedSchemaTyped
import example.dao.Dao.{IdMapping, MappedEntity, Resource, ResourceId, ResourceType}
import zio.dynamodb.ProjectionExpression.$
import zio.{Chunk, ULayer, ZIO, stream}
import zio.dynamodb.{AttrMap, AttributeValue, ConditionExpression, DynamoDBExecutor, DynamoDBQuery, Item, KeyConditionExpr, LastEvaluatedKey}



object Dao {
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

  case class Resource(resourceType: ResourceType, id: ResourceId){
    def flatPrefix = s"${resourceType.value}#${id.value}"
  }

  object Resource {
    def fromValues(prefix: String, id: String): Resource = Resource(ResourceType(prefix), ResourceId(id))
  }

  case class ResourceType(value: String) extends AnyVal
  case class ResourceId(value: String) extends AnyVal

  // given resource type and an object(entity) of this type we could generate a resource id (or extract it from the entity if it could be derived from the entity itself


  case class MappedEntity(resourcePrefix: ResourceType, id: ResourceId, fields: Map[String, Field], parent: Option[MappedEntity]) {
    def sk(time: Long) = s"${resourcePrefix.value}#${id.value}#$time"
  }

  // when a new entity is being persisted there could be no id yet, there has to be a function that gives us the id
  // based on the resource prefix, we should be able to get this information after parsing the specs

}

case class Repository(tableName: String,
                      resourceMappings: Vector[IdMapping])
                     (executor: ULayer[DynamoDBExecutor]) {

  def save(entity: MappedEntity): ZIO[Any, Throwable, Option[Item]] = {
    val attrMap: AttrMap = toAttrMap(entity, System.currentTimeMillis())
    DynamoDBQuery.putItem(tableName, attrMap).execute.provideLayer(executor)
  }

  def saveTyped[T: ProcessedSchemaTyped](value: T): ZIO[Any, Throwable, Option[Item]] = {
    val attrs = implicitly[ProcessedSchemaTyped[T]].toAttrMap(value)
    DynamoDBQuery.putItem(tableName, attrs).execute.provideLayer(executor)
  }

  def list[T: ProcessedSchemaTyped](parent: String): ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === parent && $("sk").sortKey.beginsWith(prefix))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  /**
   * we can either have a slow version that fetches all the items and then filters them by the sort key prefix
   * or use GSI
   * @tparam T
   * @return
   */
  def scanAll[T: ProcessedSchemaTyped] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val x = DynamoDBQuery.scanSomeItem(tableName, 10)
      .where($("sk").beginsWith(prefix))
      .execute.provideLayer(executor)

    x.map(_._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  def readTyped[T: ProcessedSchemaTyped](id: String, parent: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === parent && $("sk").sortKey.beginsWith(prefix + "#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
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
