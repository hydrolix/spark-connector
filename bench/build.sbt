//noinspection SbtDependencyVersionInspection -- JDK8 stuff :/
libraryDependencies ++= List(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.hadoop" % "hadoop-common" % "3.3.5",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.12",
  "com.google.cloud" % "google-cloud-storage" % "2.20.2",
  "ch.qos.logback" % "logback-classic" % "1.3.6",
  "net.openhft" % "zero-allocation-hashing" % "0.16",
)
