libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",

  "com.github.bigwheel" %% "util-backports" % "2.1",
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
  "net.openhft" % "zero-allocation-hashing" % "0.16",
)

