package example.dao

import example.SchemaParser.ProcessedSchemaTyped
import example.dao.Dao.{IdMapping, MappedEntity, Resource, ResourceId, ResourceType}
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SchemaUtils.Timestamp
import zio.{Chunk, ULayer, ZIO, stream}
import zio.dynamodb.{AttrMap, AttributeValue, ConditionExpression, DynamoDBExecutor, DynamoDBQuery, Item, KeyConditionExpr, LastEvaluatedKey, ProjectionExpression}

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

case class Repository(tableName: String)
                     (executor: ULayer[DynamoDBExecutor]) {

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
  def scanAll[T: ProcessedSchemaTyped]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val x = DynamoDBQuery.scanSomeItem(tableName, 10)
      .where($("sk").beginsWith(prefix))
      .execute.provideLayer(executor)

    x.map(_._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  def listAll[T: ProcessedSchemaTyped]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    for {
      x <- DynamoDBQuery.querySomeItem(tableName, 10) //todo: figure out why querySomeItems does not work - is there a difference in projections
        .whereKey($("gsi_pk1").partitionKey === prefix)
        .indexName("gsi1")
        .execute.provideLayer(executor)
    } yield x._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    }
  }

  def readTyped[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .indexName("gsi1")
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith(id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  def readHistory[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, List[(T, Timestamp)]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .indexName("gsi1")
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith(id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.toList.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMapWithTimestamp(item)
    })
  }

  def readTypedByParent[T: ProcessedSchemaTyped](id: String, parent: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, 10)
      .whereKey($("pk").partitionKey === parent && $("sk").sortKey.beginsWith(prefix + "#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }


}
