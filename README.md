running `sbt compile` results in the following error:
```
CreateTable.scala:3:21: object AttributeDefinition in package dynamodb cannot be accessed in package zio.dynamodb
[error] import zio.dynamodb.AttributeDefinition._
```