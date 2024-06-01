ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.4.0"

lazy val root = (project in file("."))
  .aggregate(main, example)

lazy val Versions = new {
  val zio = "2.0.21"
  val zioHttp = "3.0.0-RC8"
  val zioDynamodb = "1.0.0-RC3"

  val zioSchemaDerivation = "1.1.1"
  val zioSchemaJson = "1.1.1"
}

lazy val main = project
  .settings(
    name := "reproduce",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-http" % Versions.zioHttp,
      "dev.zio" %% "zio" % Versions.zio,
      "dev.zio" %% "zio-dynamodb" % Versions.zioDynamodb,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.2" % Test,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "dev.zio" %% "zio-schema-derivation" % Versions.zioSchemaDerivation
    ),
    //    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided"
  )

// 'example' project
lazy val example = project
  .settings(
    name := "example",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % Versions.zio,
      "dev.zio" %% "zio-schema-derivation" % Versions.zioSchemaDerivation,
      "dev.zio" %% "zio-dynamodb" % Versions.zioDynamodb,
      "dev.zio" %% "zio-http" % Versions.zioHttp,
      "dev.zio" %% "zio-schema-json" % Versions.zioSchemaJson,
    )
  )
  .dependsOn(main)

