name := "elastic_scaling"

organization := "com.amadeus"

ThisBuild / versionScheme := Some("semver-spec")

version := "2.0-SNAPSHOT"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark"         %% "spark-core"              % "3.4.1" % "provided",
  "org.apache.spark"         %% "spark-sql"               % "3.4.1" % "provided",
  "com.databricks"           % "databricks-sdk-java"      % "0.8.1",
  "org.scalactic"            %% "scalactic"               % "3.2.17" % "test",
  "org.scalatest"            %% "scalatest"               % "3.2.17" % "test",
  "com.holdenkarau"          %% "spark-testing-base"      % "3.4.0_1.4.3" % "test"
  
)

resolvers += "amadeus-mvn" at "https://repository.rnd.amadeus.net/mvn-built"
resolvers += "amadeus-sbt" at "https://repository.rnd.amadeus.net/sbt-built"
