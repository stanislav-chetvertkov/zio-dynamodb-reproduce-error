
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12" // works in 2.13
//ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "reproduce",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.13",
//      "dev.zio" %% "zio-dynamodb" % "0.2.13",
      "dev.zio" %% "zio-dynamodb" % "1.0.0-RC1",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.12" % Test,
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  )
