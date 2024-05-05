package configuration.dao

import configuration.ConfigSchemaCodec
import configuration.ConfigSchemaCodec.{Timestamp, Version, indexed}
import configuration.TableStructure.*
import configuration.dao.Repository.Config
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.*
import zio.schema.Schema
import zio.{Chunk, ZIO, ZLayer}

case class Repository(config: Config, executor: DynamoDBExecutor, chunkSize: Int){
  val tableName: String = config.tableName

  def queryByParameter[T: Schema : ConfigSchemaCodec, A](partitionField: Schema.Field[T, A], value: A)
                                                        (implicit t: ToAttributeValue[A])
  : ZIO[Any, Throwable, List[T]] = {
    val indexNameOpt = partitionField.annotations.collectFirst {
      case i:indexed[_] => i
    }

    val index = indexNameOpt.getOrElse(throw new RuntimeException(s"Column name not found for $partitionField"))

    val query = DynamoDBQuery.queryAllItem(tableName) //todo:has to accomodate for history
      .indexName(index.indexName.name)
      .whereKey($(index.pkName).partitionKey === value && $(index.skName).sortKey === value)

    val items: ZIO[Any, Throwable, Chunk[Item]] = query.execute.flatMap(_.runCollect)
      .provideLayer(ZLayer.succeed(executor))
    val proc = implicitly[ConfigSchemaCodec[T]]
    items.map(_.map { item =>
      proc.fromAttrMap(item)
    }.toList)
  }

  // I guess I'd need to maintain history in a separate sorting key structure
  // on the initial write record 2 entries one with the current version and one history 'event'
  def save[T: Schema : ConfigSchemaCodec](value: T, currentVersion: Option[Int] = None): ZIO[Any, Throwable, Option[Item]] = {
    val schemaProcessor = implicitly[ConfigSchemaCodec[T]]
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
          .transaction.execute.provideLayer(ZLayer.succeed(executor))
        putItem

      case None =>
        val attrs = schemaProcessor.toAttrMap(value, 1)
        val attrsHistoric = schemaProcessor.toAttrMap(value, 1, isHistory = true) //write to history sk

        DynamoDBQuery.putItem(tableName, attrsHistoric)
          .zipRight(
            DynamoDBQuery.putItem(tableName, attrs).where($("version").notExists)
          )
          .transaction.execute.provideLayer(ZLayer.succeed(executor))
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

  // list resource items by parent
  def list[T: ConfigSchemaCodec](parent: String): ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ConfigSchemaCodec[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.queryAllItem(tableName)
      .whereKey($(PK).partitionKey === parent && $(SK).sortKey.beginsWith(prefix))

    val items: ZIO[Any, Throwable, Chunk[Item]] = query.execute.flatMap(_.runCollect)
      .provideLayer(ZLayer.succeed(executor))

    items.map(_.map { item => proc.fromAttrMap(item) })
  }

  //todo: add list by parent version with pagination


  // uses GSI
  def listAll[T: ConfigSchemaCodec]: ZIO[Any, Throwable, Chunk[T]] = {
    val proc = implicitly[ConfigSchemaCodec[T]]
    val prefix = proc.resourcePrefix

    for {
      x <- DynamoDBQuery.queryAllItem(tableName) //todo: figure out why querySomeItems does not work - is there a difference in projections
        .whereKey($(GSI_PK).partitionKey === prefix && $(GSI_SK).sortKey.beginsWith(GSI_VALUES_PREFIX + SEPARATOR))
        .indexName(GSI_INDEX_NAME)
        .execute.flatMap(_.runCollect)
        .provideLayer(ZLayer.succeed(executor))
    } yield x.map { item =>
      implicitly[ConfigSchemaCodec[T]].fromAttrMap(item)
    }
  }

  // read by resource id (without specifying the parent)
  def read[T: ConfigSchemaCodec](id: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ConfigSchemaCodec[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, chunkSize)
      .indexName(GSI_INDEX_NAME)
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("values#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute
      .provideLayer(ZLayer.succeed(executor))

    x.map(_._1.headOption.map { item =>
      implicitly[ConfigSchemaCodec[T]].fromAttrMap(item)
    })
  }

  def readHistory[T: ConfigSchemaCodec](id: String): ZIO[Any, Throwable, List[(T, Version, Timestamp)]] = {
    val proc = implicitly[ConfigSchemaCodec[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.queryAllItem(tableName)
      .indexName(GSI_INDEX_NAME)
      .whereKey($("gsi_pk1").partitionKey === prefix && $("gsi_sk1").sortKey.beginsWith("history#" + id))

    val x: ZIO[Any, Throwable, Chunk[Item]] = query.execute.flatMap(_.runCollect)
      .provideLayer(ZLayer.succeed(executor))

    x.map(_.toList.map { item =>
      implicitly[ConfigSchemaCodec[T]].fromAttrMapWithTimestamp(item)
    })
  }

  def readTypedByParent[T: ConfigSchemaCodec](id: String, parent: String): ZIO[Any, Throwable, Option[T]] = {
    val proc = implicitly[ConfigSchemaCodec[T]]
    val prefix = proc.resourcePrefix

    val query = DynamoDBQuery.querySomeItem(tableName, chunkSize)
      .whereKey($(PK).partitionKey === parent && $(SK).sortKey.beginsWith(prefix + "#" + id))

    val x: ZIO[Any, Throwable, (Chunk[Item], LastEvaluatedKey)] = query.execute
      .provideLayer(ZLayer.succeed(executor))

    val result: ZIO[Any, Throwable, Option[T]] = x.map(_._1.headOption.map { item =>
      implicitly[ConfigSchemaCodec[T]].fromAttrMap(item)
    })

    result
  }
}

object Repository {
  case class Config(tableName: String)
  val live: ZLayer[Config & DynamoDBExecutor, Nothing, Repository] = ZLayer.fromZIO(
    for {
      config <- ZIO.service[Config]
      executor <- ZIO.service[DynamoDBExecutor]
    } yield Repository(config, executor, chunkSize = 10)
  )
}
