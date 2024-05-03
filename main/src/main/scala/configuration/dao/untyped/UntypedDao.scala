package configuration.dao.untyped

import configuration.SchemaParser.ProcessedSchemaTyped
import configuration.dao.Dao.{IdMapping, MappedEntity, Resource, ResourceId, ResourceType}
import zio.dynamodb.ProjectionExpression.$
import zio.{Chunk, ULayer, ZIO, stream}
import zio.dynamodb.{AttrMap, AttributeValue, ConditionExpression, DynamoDBExecutor, DynamoDBQuery, Item, KeyConditionExpr, LastEvaluatedKey, ProjectionExpression}


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
