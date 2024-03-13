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
      BillingMode.provisioned(readCapacityUnit = 100, writeCapacityUnit = 100)
    )(
      attrDefnString("pk"),
      attrDefnString("sk"),
      attrDefnString("gsi_pk1"),
      attrDefnString("gsi_sk1")
    )
      .gsi(
        "gsi1",
        KeySchema(hashKey = "gsi_pk1", sortKey = "gsi_sk1"),
        ProjectionType.All,
        readCapacityUnit = 100,
        writeCapacityUnit = 100
      )
//      .gsi(
//        "gsi2",
//        KeySchema(hashKey = "gsi_pk2", sortKey = "gsi_sk2"),
//        ProjectionType.All,
//        readCapacityUnit = 100,
//        writeCapacityUnit = 100
//      )


}
