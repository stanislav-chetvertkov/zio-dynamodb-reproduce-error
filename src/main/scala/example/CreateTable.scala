package example

import zio.dynamodb.AttributeDefinition._
import zio.dynamodb.DynamoDBQuery.createTable
import zio.dynamodb._

object CreateTable {
  val TableName = "mappings"
  val createTableExample =
    createTable(
      tableName = TableName,
      keySchema = KeySchema(hashKey = "pk", sortKey = "sk"),
      BillingMode.provisioned(readCapacityUnit = 10, writeCapacityUnit = 10)
    )(
      attrDefnString("pk"),
      attrDefnString("sk")
    )
//      .gsi(
//        "indexName",
//        KeySchema("key2", "sortKey2"),
//        ProjectionType.Include("nonKeyField1", "nonKeyField2"),
//        readCapacityUnit = 10,
//        writeCapacityUnit = 10
//      )


}
