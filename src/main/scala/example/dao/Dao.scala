package example.dao

import example.SchemaParser.{GSI_INDEX_NAME, GSI_PK, GSI_SK, GSI_VALUES_PREFIX, PK, ProcessedSchemaTyped, SEPARATOR, SK}
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

  // list resource items by parent
  def list[T: ProcessedSchemaTyped](parent: String): ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.queryAllItem(tableName)
      .whereKey($(PK).partitionKey === parent && $(SK).sortKey.beginsWith(prefix))

    val items: ZIO[Any, Throwable, Chunk[Item]] = query.execute.flatMap(_.runCollect).provideLayer(executor)

    items.map(_.map { item => proc.fromAttrMap(item) })
  }

  // uses GSI
  def listAll[T: ProcessedSchemaTyped]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    for {
      x <- DynamoDBQuery.queryAllItem(tableName) //todo: figure out why querySomeItems does not work - is there a difference in projections
        .whereKey($(GSI_PK).partitionKey === prefix && $(GSI_SK).sortKey.beginsWith(GSI_VALUES_PREFIX + SEPARATOR))
        .indexName(GSI_INDEX_NAME)
        .execute.flatMap(_.runCollect).provideLayer(executor)
    } yield x.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    }
  }

  // read by resource id (without specifying the parent)
  def read[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .indexName(GSI_INDEX_NAME)
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("values#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })
  }

  def readHistory[T: ProcessedSchemaTyped](id: String): ZIO[Any, Throwable, List[(T, Version, Timestamp)]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.queryAllItem(tableName)
      .indexName(GSI_INDEX_NAME)
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("history#" + id))

    val x: ZIO[Any, Throwable, Chunk[Item]] = query.execute.flatMap(_.runCollect).provideLayer(executor)

    x.map(_.toList.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMapWithTimestamp(item)
    })
  }

  def readTypedByParent[T: ProcessedSchemaTyped](id: String, parent: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ProcessedSchemaTyped[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, ChunkSize)
      .whereKey($(PK).partitionKey === parent && $(SK).sortKey.beginsWith(prefix + "#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute.provideLayer(executor)

    val result: ZIO[Any, Throwable, Option[T]] = x.map(_._1.headOption.map { item =>
      implicitly[ProcessedSchemaTyped[T]].fromAttrMap(item)
    })

    result
  }
}
