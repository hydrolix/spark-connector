ThisBuild / organization := "io.hydrolix"
ThisBuild / version := "0.9.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17"

lazy val commonSettings = Seq(
  javacOptions := Seq("-source", "8", "-target", "8"),
  scalacOptions := Seq("-target:jvm-8")
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "hdx-spark",
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(model, bench, connector)

lazy val assemblySettings = Seq(
  assemblyShadeRules := Seq(
    ShadeRule.rename("com.github.benmanes.caffeine.**" -> "shadecaffeine.@1").inAll,
    ShadeRule.rename("com.fasterxml.jackson.**" -> "shadejackson.@1").inAll
  ),
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
)

lazy val model = (project in file("model"))
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)

lazy val bench = (project in file("bench"))
  .settings(commonSettings)
  .disablePlugins(AssemblyPlugin)
  .dependsOn(model)

lazy val connector = (project in file("connector"))
  .dependsOn(model)
  .settings(commonSettings ++ assemblySettings)
