ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "hdx-spark",
    idePackagePrefix := Some("io.hydrolix.spark")
  )

//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.5",
  "com.zaxxer" % "HikariCP" % "5.0.1",
  "com.breinify" % "brein-time-utilities" % "1.8.0",
)
