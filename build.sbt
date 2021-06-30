name := "elastic_scaling"

organization := "com.amadeus"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark"         %% "spark-core"              % "2.4.4" % "provided",
  "org.apache.spark"         %% "spark-sql"               % "2.4.4" % "provided",
  "com.typesafe"             % "config"                   % "1.4.1",
  "org.scalactic"            %% "scalactic"               % "3.0.1" % "test",
  "org.scalatest"            %% "scalatest"               % "3.0.1" % "test",
  "com.holdenkarau"          %% "spark-testing-base"      % "2.4.4_0.14.0" % "test"
  
)

resolvers += "amadeus-mvn" at "https://repository.rnd.amadeus.net/mvn-built"
resolvers += "amadeus-sbt" at "https://repository.rnd.amadeus.net/sbt-built"
