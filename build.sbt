ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "hdx-spark"
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList(pl @ _*) if pl.last == "module-info.class"                      => MergeStrategy.discard
  case PathList("com", "clickhouse", "client", "data", "JsonStreamUtils.class") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties")                     => MergeStrategy.first
  case PathList("META-INF", "native", _*)                                       => MergeStrategy.first
  case "application.conf"                                                       => MergeStrategy.concat
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

//libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.5",
  "com.zaxxer" % "HikariCP" % "5.0.1",
  "com.breinify" % "brein-time-utilities" % "1.8.0",
  "com.google.cloud" % "google-cloud-storage" % "2.20.2",
  "ch.qos.logback" % "logback-classic" % "1.4.6",
  "org.typelevel" %% "spire" % "0.18.0",
  "net.openhft" % "zero-allocation-hashing" % "0.16"
)
