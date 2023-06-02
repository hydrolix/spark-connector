ThisBuild / organization := "io.hydrolix"
ThisBuild / version := "1.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17"

name := "hydrolix-spark-connector"

javacOptions := Seq("-source", "8", "-target", "8")
scalacOptions := Seq("-target:jvm-8")

assemblyShadeRules := Seq(
  ShadeRule.rename("com.github.benmanes.caffeine.**" -> "shadecaffeine.@1").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shadejackson.@1").inAll
)

assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

assemblyMergeStrategy := {
  case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
  case PathList(pl@_*) if pl.last == "public-suffix-list.txt" => MergeStrategy.discard
  case PathList("com", "clickhouse", "client", "data", "JsonStreamUtils.class") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "native", _*) => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

//noinspection SpellCheckingInspection
libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,

  "com.clickhouse" % "clickhouse-jdbc" % "0.4.6",
  "com.zaxxer" % "HikariCP" % "5.0.1",

  "com.github.bigwheel" %% "util-backports" % "2.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  //noinspection SbtDependencyVersionInspection -- JDK8
  "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3",
  //noinspection SbtDependencyVersionInspection -- JDK8
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  //noinspection SbtDependencyVersionInspection -- JDK8
  "ch.qos.logback" % "logback-classic" % "1.3.6",
  "net.openhft" % "zero-allocation-hashing" % "0.16",

  // Testing...
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.apache.spark" %% "spark-catalyst" % "3.3.2" % Test,
)
