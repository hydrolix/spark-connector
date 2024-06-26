package io.hydrolix.connectors.spark

import java.net.URI
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkExpressions
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.{Ignore, Test}

import io.hydrolix.connectors
import io.hydrolix.connectors.expr
import io.hydrolix.connectors.expr.{And, GreaterEqual, LessEqual, TimestampLiteral}
import io.hydrolix.connectors.spark.partitionreader.SparkRowPartitionReader
import io.hydrolix.connectors.types.TimestampType

@Ignore("requires environment not available in CI etc.")
class ConnectorSmokeTest {
  @Test
  def doStuff(): Unit = {
    val jdbcUrl = System.getenv("HDX_SPARK_JDBC_URL")
    val apiUrl = System.getenv("HDX_SPARK_API_URL")
    val user = System.getenv("HDX_USER")
    val pass = System.getenv("HDX_PASSWORD")
    val cloudCred1 = System.getenv("HDX_SPARK_CLOUD_CRED_1")
    val cloudCred2 = Option(System.getenv("HDX_SPARK_CLOUD_CRED_2"))
    val dockerImage = Option(System.getenv("HDX_DOCKER_IMAGE"))
    val info = connectors.HdxConnectionInfo(jdbcUrl, user, pass, new URI(apiUrl), None, cloudCred1, cloudCred2, dockerImage)
    val catalog = new SparkTableCatalog()
    catalog.initialize("hdx-test", new CaseInsensitiveStringMap(info.asMap.asJava))
    val table = catalog.loadTable(Identifier.of(Array("hydro"), "logs")).asInstanceOf[SparkTable]

    val now = Instant.now()
    val fiveMinutesAgo = now.minus(5L, ChronoUnit.MINUTES)

    val pred = And(
      GreaterEqual(
        expr.GetField(table.coreTable.primaryKeyField, TimestampType(3)),
        TimestampLiteral(fiveMinutesAgo)
      ),
      LessEqual(
        expr.GetField(table.coreTable.primaryKeyField, TimestampType(3)),
        TimestampLiteral(now)
      )
    )

    val sparkPred = SparkExpressions.coreToSpark(pred).asInstanceOf[Predicate]

    val sb = new SparkScanBuilder(info, table.coreTable)
    sb.pruneColumns(StructType(Nil))
    sb.pushPredicates(Array(sparkPred))

    val scan = sb.build()
    val batch = scan.toBatch
    val partitions = batch.planInputPartitions()
    println(partitions.size)

    val ssp = partitions.head.asInstanceOf[SparkScanPartition]
    val storage = table.coreTable.storages.getOrElse(ssp.coreScan.storageId, sys.error(s"No storage #${ssp.coreScan.storageId}"))

    val reader = new SparkRowPartitionReader(info, storage, ssp)
    while (reader.next()) {
      val row = reader.get()
      println(row)
    }
  }
}
