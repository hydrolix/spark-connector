package io.hydrolix.spark.connector

import java.net.URI
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._

import org.apache.spark.sql.HdxPushdown.{GetField, Literal}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.Test

import io.hydrolix.spark.connector.partitionreader.RowPartitionReader
import io.hydrolix.spark.model.HdxConnectionInfo

class ConnectorSmokeTest {
  @Test
  def doStuff(): Unit = {
    val jdbcUrl = System.getenv("HDX_SPARK_JDBC_URL")
    val apiUrl = System.getenv("HDX_SPARK_API_URL")
    val user = System.getenv("HDX_USER")
    val pass = System.getenv("HDX_PASSWORD")
    val cloudCred1 = System.getenv("HDX_SPARK_CLOUD_CRED_1")
    val cloudCred2 = Option(System.getenv("HDX_SPARK_CLOUD_CRED_2"))
    val info = HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), None, cloudCred1, cloudCred2, Some("myubuntu"))
    val catalog = new HdxTableCatalog()
    catalog.initialize("hdx-test", new CaseInsensitiveStringMap(info.asMap.asJava))
    val table = catalog.loadTable(Identifier.of(Array("hydro"), "logs")).asInstanceOf[HdxTable]

    val now = Instant.now()
    val fiveMinutesAgo = now.minus(5L, ChronoUnit.MINUTES)

    val sb = new HdxScanBuilder(info, table)
    sb.pruneColumns(StructType(Nil))
    sb.pushPredicates(Array(new And(
      new Predicate(">=", Array(GetField(table.primaryKeyField), Literal(fiveMinutesAgo))),
      new Predicate("<=", Array(GetField(table.primaryKeyField), Literal(now))),
    )))

    val scan = sb.build()
    val batch = scan.toBatch
    val partitions = batch.planInputPartitions()
    println(partitions.size)

    val reader = new RowPartitionReader(info, table.storage, table.primaryKeyField, partitions.head.asInstanceOf[HdxScanPartition])
    while (reader.next()) {
      val row = reader.get()
      println(row)
    }
  }
}
