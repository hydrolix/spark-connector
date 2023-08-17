/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
ThisBuild / organization := "io.hydrolix"
ThisBuild / version := "1.2.0-SNAPSHOT"
ThisBuild / organizationHomepage := Some(url("https://hydrolix.io/"))
ThisBuild / homepage := Some(url("https://github.com/hydrolix/spark-connector/"))
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  "Proprietary" -> new URL("https://github.com/hydrolix/spark-connector/#proprietary"),
)
ThisBuild / crossScalaVersions := List("2.12.18", "2.13.11")

name := "hydrolix-spark-connector"

javacOptions := Seq("-source", "11", "-target", "11")

assemblyShadeRules := Seq(
  ShadeRule.rename("com.github.benmanes.caffeine.**" -> "shadecaffeine.@1").inAll,
  ShadeRule.rename("com.fasterxml.jackson.**" -> "shadejackson.@1").inAll
)
assembly / assemblyJarName := s"${name.value}-assembly_${scalaBinaryVersion.value}-${version.value}.jar"
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

assemblyMergeStrategy := {
  case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
  case PathList(pl@_*) if pl.last == "public-suffix-list.txt" => MergeStrategy.discard
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
  "com.google.guava" % "guava" % "32.0.0-jre",

  "com.github.bigwheel" %% "util-backports" % "2.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  "com.github.ben-manes.caffeine" % "caffeine" % "3.1.5",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "net.java.dev.jna" % "jna" % "5.13.0", // for Wyhash

  // Testing...
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Test,
)

credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credentials")

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/hydrolix/spark-connector"),
    "scm:git@github.com:hydrolix/spark-connector.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "acruise",
    name = "Alex Cruise",
    email = "alex@hydrolix.io",
    url = url("https://github.com/acruise")
  )
)
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true