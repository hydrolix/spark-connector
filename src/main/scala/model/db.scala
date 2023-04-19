package io.hydrolix.spark
package model

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.Logger

import java.net.URI
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

case class HdxConnectionInfo(orgId: UUID,
                             jdbcUrl: String,
                             user: String,
                             password: String,
                             apiUrl: URI,
                             turbineIniPath: String,
                             turbineCmdPath: String,
                             bucketPrefix: String,
                             cloud: String,
                             cloudCred1: String,
                             cloudCred2: String)
{
  val asMap: Map[String, String] = {
    import HdxConnectionInfo._

    Map(
      OPT_ORG_ID -> orgId.toString,
      OPT_JDBC_URL -> jdbcUrl,
      OPT_USERNAME -> user,
      OPT_PASSWORD -> password,
      OPT_API_URL -> apiUrl.toString,
      OPT_TURBINE_INI_PATH -> turbineIniPath,
      OPT_TURBINE_CMD_PATH -> turbineCmdPath,
      OPT_CLOUD -> cloud,
      OPT_BUCKET_PREFIX -> bucketPrefix,
      OPT_CLOUD_CRED1 -> cloudCred1,
      OPT_CLOUD_CRED2 -> cloudCred2,
    )
  }
}

object HdxConnectionInfo {
  val OPT_ORG_ID           = "org_id"
  val OPT_PROJECT_NAME     = "project_name"
  val OPT_TABLE_NAME       = "table_name"
  val OPT_JDBC_URL         = "jdbc_url"
  val OPT_USERNAME         = "username"
  val OPT_PASSWORD         = "password"
  val OPT_API_URL          = "api_url"
  val OPT_TURBINE_INI_PATH = "turbine_ini_path"
  val OPT_TURBINE_CMD_PATH = "turbine_cmd_path"
  val OPT_CLOUD            = "cloud"
  val OPT_BUCKET_PREFIX    = "bucket_prefix"
  val OPT_CLOUD_CRED1      = "cloud_credential_1"
  val OPT_CLOUD_CRED2      = "cloud_credential_2"

  private def req(options: CaseInsensitiveStringMap, name: String): String = {
    val s = options.get(name)
    if (s == null) sys.error(s"$name is required")
    s
  }

  private def opt(options: CaseInsensitiveStringMap, name: String): Option[String] = {
    Option(options.get(name))
  }

  def fromOpts(options: CaseInsensitiveStringMap, logger: Logger): HdxConnectionInfo = {
    val orgId = UUID.fromString(req(options, OPT_ORG_ID))
    val url = req(options, OPT_JDBC_URL)
    val user = req(options, OPT_USERNAME)
    val pass = req(options, OPT_PASSWORD)
    val apiUrl = new URI(req(options, OPT_API_URL))
    val turbineIni = Path.of(req(options, OPT_TURBINE_INI_PATH))
    val turbineCmd = Path.of(req(options, OPT_TURBINE_CMD_PATH))
    val bucketPrefix = req(options, OPT_BUCKET_PREFIX)
    val cloud = req(options, OPT_CLOUD)
    val cred1 = req(options, OPT_CLOUD_CRED1)
    val cred2 = opt(options, OPT_CLOUD_CRED2).getOrElse("-")

    if (!turbineIni.toFile.exists()) logger.warn(s"$OPT_TURBINE_INI_PATH $turbineIni does not exist")
    if (!turbineCmd.toFile.exists()) logger.warn(s"$OPT_TURBINE_CMD_PATH $turbineCmd does not exist")

    HdxConnectionInfo(orgId, url, user, pass, apiUrl, turbineIni.toString, turbineCmd.toString, bucketPrefix, cloud, cred1, cred2)
  }

}

case class HdxColumnInfo(name: String,
                      colType: Int,
                     typeName: String,
                     nullable: Boolean,
                    sparkType: DataType)

case class HdxDbPartition(partition: String,
                          minTimestamp: Instant,
                          maxTimestamp: Instant,
                          manifestSize: Long,
                          dataSize: Long,
                          indexSize: Long,
                          rows: Long,
                          memSize: Long,
                          rootPath: String,
                          shardKey: String,
                          active: Boolean)

