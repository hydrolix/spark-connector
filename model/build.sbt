libraryDependencies ++= List(
  "org.apache.spark" %% "spark-catalyst" % "3.4.0" % "provided",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  "ch.qos.logback" % "logback-classic" % "1.4.6",
)
