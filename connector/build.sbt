//noinspection SbtDependencyVersionInspection
libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,

  "com.github.bigwheel" %% "util-backports" % "2.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2",
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.14.2",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.14.2",
  "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3", // Need older version for JDK8 bytecode :/
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1", // java.net client not available in JDK8 :/
  "ch.qos.logback" % "logback-classic" % "1.4.6",
  "net.openhft" % "zero-allocation-hashing" % "0.16",

  // Testing...
  "com.github.sbt" % "junit-interface" % "0.13.2" % Test,
  "org.apache.spark" %% "spark-catalyst" % "3.3.2" % Test,
)
