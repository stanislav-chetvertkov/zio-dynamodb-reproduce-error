ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "reproduce",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.21",
      //      "dev.zio" %% "zio-dynamodb" % "0.2.13",
      "dev.zio" %% "zio-dynamodb" % "1.0.0-RC2",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.2" % Test,
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    ),
    libraryDependencies += "dev.zio" %% "zio-schema-derivation" % "0.4.17",
    libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided"
  )
