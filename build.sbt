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
ThisBuild / organizationHomepage := Some(url("https://hydrolix.io/"))
ThisBuild / homepage := Some(url("https://github.com/hydrolix/spark-connector/"))
ThisBuild / licenses := List(
  "Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  "Proprietary" -> url("https://github.com/hydrolix/spark-connector/#proprietary"),
)
ThisBuild / crossScalaVersions := List("2.12.19", "2.13.12")

name := "hydrolix-spark-connector"

javacOptions := Seq("-source", "11", "-target", "11")

val sparkVersion = "3.4.2"

resolvers ++= Resolver.sonatypeOssRepos("public")

lazy val main = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .settings(
    libraryDependencies ++= List(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,

      "io.hydrolix" %% "hydrolix-connectors-core" % "1.5.0",

      // Testing...
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
    ),
  )

lazy val assembled = project
  .enablePlugins(AssemblyPlugin)
  .dependsOn(main)
  .settings(
    name := "hydrolix-spark-connector-assembly",
    Compile / packageBin := assembly.value,
      assembly / assemblyShadeRules := Seq(
      ShadeRule.rename(
        "com.github.benmanes.caffeine.**" -> "shadecaffeine.@1",
        "com.fasterxml.jackson.**"        -> "shadejackson.@1",
        "com.google.**"                   -> "shadegoogle.@1",
        "org.apache.hc.**"                -> "shadehttpclient.@1",
        "fastparse.**"                    -> "shadefastparse.@1",
        "geny.**"                         -> "shadegeny.@1",
        "com.zaxxer.hikari.**"            -> "shadehikari.@1",
        "com.typesafe.**"                 -> "shadetypesafe.@1",
//      "ch.qos.logback.**"               -> "shadelogback.@1",
        "org.slf4j.**"                    -> "shadeslf4j.@1",
        "com.clickhouse.**"               -> "shadeclickhouse.@1",
        "com.sun.jna.**"                  -> "shadejna.@1",
        "com.thoughtworks.paranamer.**"   -> "shadeparanamer.@1",
        "org.checkerframework.**"         -> "shadecheckerframework.@1",
        "javax.annotation.**"             -> "shadeannotation.@1",
      ).inAll,
    ),
    assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar",
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
      case PathList(pl@_*) if pl.last == "public-suffix-list.txt" => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
      case PathList("META-INF", "native", _*) => MergeStrategy.first
      case "application.conf" => MergeStrategy.concat
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    }
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

releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseCrossBuild := true
releaseIgnoreUntrackedFiles := true