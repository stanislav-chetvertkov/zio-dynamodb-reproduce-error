package example.dao

import example.SchemaParser.{ProcessedSchemaTyped, SEPARATOR}
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import zio.dynamodb.DynamoDBError.AWSError
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.SchemaUtils.{Timestamp, Version}
import zio.{Chunk, ULayer, ZIO, stream}
import zio.dynamodb.{AttrMap, AttributeValue, DynamoDBError, DynamoDBExecutor, DynamoDBQuery, Item, KeyConditionExpr, LastEvaluatedKey, PrimaryKey, ProjectionExpression, TableName, UpdateExpression}
import zio.schema.Schema

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

  case class Resource(resourceType: ResourceType, id: ResourceId) {
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

  def listAll(): ZIO[Any, DynamoDBError, stream.Stream[Throwable, Item]] = {
    DynamoDBQuery.scanAllItem(tableName, ProjectionExpression.some).execute.provideLayer(executor)
  }

  // I guess I'd need to maintain history in a separate sorting key structure
  // on the initial write record 2 entries one with the current version and one history 'event'
  def save[T: Schema : ProcessedSchemaTyped](value: T, currentVersion: Option[Int] = None): ZIO[Any, Throwable, Option[Item]] = {
    val schemaProcessor = implicitly[ProcessedSchemaTyped[T]]
    val prefix = schemaProcessor.resourcePrefix
    val parentId = schemaProcessor.parentId(value)

    val id = schemaProcessor.resourceId(value)

    val query = currentVersion match {
      case Some(version) =>
        val attrs = schemaProcessor.toAttrMap(value, version + 1)
        val attrsHistoric = schemaProcessor.toAttrMap(value, version + 1, isHistory = true)
        val putHistoryItem = DynamoDBQuery.putItem(tableName, attrsHistoric)

        val putItem: ZIO[Any, Throwable, Option[Item]] = DynamoDBQuery.putItem(tableName, attrs)
          .where($("version") === currentVersion).zipRight(putHistoryItem)
          .transaction.execute.provideLayer(executor)
        putItem

      case None =>
        val attrs = schemaProcessor.toAttrMap(value, 1)
        val attrsHistoric = schemaProcessor.toAttrMap(value, 1, isHistory = true) //write to history sk

        DynamoDBQuery.putItem(tableName, attrsHistoric)
          .zipRight(
            DynamoDBQuery.putItem(tableName, attrs).where($("version").notExists)
          )
        .transaction.execute.provideLayer(executor)
    }

    query.catchAll({
      case e@DynamoDBError.AWSError(th) if th.toString.contains("TransactionCanceledException") =>
        ZIO.logWarning(s"Transaction cancelled for $e") *>
          ZIO.none
      case other =>
        ZIO.fail(other)
    })
  }

  // resource_type#id with 'version' column
  // resource_type#history#id#timestamp

  private val ChunkSize = 10

  /**
   * we can either have a slow version that fetches all the items and then filters them by the sort key prefix
   * or use GSI
   *
   * @tparam T
   * @return
   */
  def scanAll[T: ProcessedSchemaTyped]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val x = DynamoDBQuery.scanSomeItem(tableName, ChunkSize)
      .where($("sk").beginsWith(prefix))
      .execute.provideLayer(executor)

    x.map(_._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  // list resource items by parent
  def list[T: ProcessedSchemaTyped](parent: String): ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .whereKey($("pk").partitionKey === parent && $("sk").sortKey.beginsWith(prefix))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    val result: ZIO[Any, Throwable, Chunk[T]] = x.map(_._1.map { item =>
      proc.fromAttrMap(item)
    })

    //todo: filter out history items
    result
  }

  // uses GSI
  def listAll[T: ProcessedSchemaTyped]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    for {
      x <- DynamoDBQuery.querySomeItem(tableName, ChunkSize) //todo: figure out why querySomeItems does not work - is there a difference in projections
        .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("values" + SEPARATOR))
        .indexName("gsi1")
        .execute.provideLayer(executor)
    } yield x._1.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    }
  }

  // read by resource id (without specifying the parent)
  def read[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .indexName("gsi1")
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("values#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  def readHistory[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, List[(T, Version, Timestamp)]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .indexName("gsi1")
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("history#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.toList.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMapWithTimestamp(item)
    })
  }

  def readTypedByParent[T: ProcessedSchemaTyped](id: String, parent: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .whereKey($("pk").partitionKey === parent && $("sk").sortKey.beginsWith(prefix + "#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    val result: ZIO[Any, Throwable, Option[T]] = x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })

    result
  }
}
