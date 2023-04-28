package io.hydrolix.spark.connector

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.Logger

import java.net.URI
import java.nio.file.Path
import java.util.UUID

/**
 * All the information we need to connect to Hydrolix stuff
 * @param orgId          the UUID of the organization we need to authenticate as
 * @param jdbcUrl        the JDBC URL to connect to the Hydrolix query head, e.g. `jdbc:clickhouse://host:port?ssl=true`
 * @param user           the username to authenticate to JDBC and the Hydrolix API
 * @param password       the password for authentication to JDBC and the Hydrolix API
 * @param apiUrl         the URL of the Hydrolix API; probably needs to end with `/config/v1/` at the moment.
 * @param turbineIniPath the filesystem path to a `turbine.ini` file we'll use to connect to cloud storage
 * @param turbineCmdPath the filesystem path to a `turbine_cmd` binary we'll use to dump partitions
 *                       (note, it needs to be built from the HDX-3779 branch)
 */
case class HdxConnectionInfo(orgId: UUID,
                           jdbcUrl: String,
                              user: String,
                          password: String,
                            apiUrl: URI,
                    turbineIniPath: String,
                    turbineCmdPath: String)
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
    )
  }
}

object HdxConnectionInfo {
  val OPT_ORG_ID = "org_id"
  val OPT_PROJECT_NAME = "project_name"
  val OPT_TABLE_NAME = "table_name"
  val OPT_JDBC_URL = "jdbc_url"
  val OPT_USERNAME = "username"
  val OPT_PASSWORD = "password"
  val OPT_API_URL = "api_url"
  val OPT_TURBINE_INI_PATH = "turbine_ini_path"
  val OPT_TURBINE_CMD_PATH = "turbine_cmd_path"

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

    if (!turbineIni.toFile.exists()) logger.warn(s"$OPT_TURBINE_INI_PATH $turbineIni does not exist")
    if (!turbineCmd.toFile.exists()) logger.warn(s"$OPT_TURBINE_CMD_PATH $turbineCmd does not exist")

    HdxConnectionInfo(orgId, url, user, pass, apiUrl, turbineIni.toString, turbineCmd.toString)
  }

}