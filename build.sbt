name := "elastic_scaling"

organization := "com.amadeus"

ThisBuild / versionScheme := Some("semver-spec")

version := "2.0.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark"         %% "spark-core"              % "3.4.1" % "provided",
  "org.apache.spark"         %% "spark-sql"               % "3.4.1" % "provided",
  "com.databricks"           % "databricks-sdk-java"      % "0.8.1",
  "org.scalactic"            %% "scalactic"               % "3.2.17" % "test",
  "org.scalatest"            %% "scalatest"               % "3.2.17" % "test",
  "com.holdenkarau"          %% "spark-testing-base"      % "3.4.0_1.4.3" % "test"
  
)
// Publishing settings
publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/AmadeusITGroup/Elastic-Scaling")

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "",
  sys.env.getOrElse("GITHUB_TOKEN", "")
)

publishMavenStyle := true
Test / publishArtifact := false

pomIncludeRepository := { _ => true }

// Artifact metadata
pomExtra :=
  <description>
    Elastic scaling is a library that allows to control the number of resources (executors) instantiated
    by a Spark Structured Streaming Job in order to optimize the effective microbatch duration.
    The goal is to use the minimum needed resources for processing the workload, in a time that is the closest
    to the microbatch duration itself, adapting the number of executors to the volume of data received.
  </description>
    <url>https://github.com/AmadeusITGroup/Elastic-Scaling</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>https://github.com/AmadeusITGroup/Elastic-Scaling/LICENSE.txt</url>
      </license>
    </licenses>
