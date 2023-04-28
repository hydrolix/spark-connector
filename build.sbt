ThisBuild / organization := "io.hydrolix"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "hdx-spark"
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(model, bench, connector)

lazy val assemblySettings = Seq(
  assemblyMergeStrategy := {
    case PathList(pl@_*) if pl.last == "module-info.class" => MergeStrategy.discard
    case PathList("com", "clickhouse", "client", "data", "JsonStreamUtils.class") => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
    case PathList("META-INF", "native", _*) => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

lazy val model = (project in file("model"))
  .disablePlugins(AssemblyPlugin)

lazy val bench = (project in file("bench"))
  .disablePlugins(AssemblyPlugin)
  .dependsOn(model)

lazy val connector = (project in file("connector"))
  .dependsOn(model)
  .settings(assemblySettings)
