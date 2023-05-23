package io.hydrolix.spark.model

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.net.URI
import java.util.UUID

/**
 * All the information we need to connect to Hydrolix stuff
 * @param orgId            the UUID of the organization we need to authenticate as
 * @param jdbcUrl          the JDBC URL to connect to the Hydrolix query head, e.g. `jdbc:clickhouse://host:port?ssl=true`
 * @param user             the username to authenticate to JDBC and the Hydrolix API
 * @param password         the password for authentication to JDBC and the Hydrolix API
 * @param apiUrl           the URL of the Hydrolix API; probably needs to end with `/config/v1/` at the moment.
 * @param cloudCred1       first credential for storage, required, e.g.:
 *                           - for `gcs`, the base64(gzip(_)) of a gcp service account key file
 *                           - for AWS, the access key ID
 * @param cloudCred2       second credential for storage, optional, e.g.:
 *                           - for `gcs`, not required
 *                           - for AWS, the secret key
 */
case class HdxConnectionInfo(orgId: UUID,
                           jdbcUrl: String,
                              user: String,
                          password: String,
                            apiUrl: URI,
                   partitionPrefix: Option[String],
                        cloudCred1: String,
                        cloudCred2: Option[String])
{
  val asMap: Map[String, String] = {
    import HdxConnectionInfo._

    Map(
      OPT_ORG_ID -> orgId.toString,
      OPT_JDBC_URL -> jdbcUrl,
      OPT_USERNAME -> user,
      OPT_PASSWORD -> password,
      OPT_API_URL -> apiUrl.toString,
      OPT_CLOUD_CRED_1 -> cloudCred1,
    ) ++
      cloudCred2.map(OPT_CLOUD_CRED_2 -> _) ++
      partitionPrefix.map(OPT_PARTITION_PREFIX -> _)
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
  val OPT_PARTITION_PREFIX = "partition_prefix"
  val OPT_CLOUD_CRED_1 = "cloud_cred_1"
  val OPT_CLOUD_CRED_2 = "cloud_cred_2"

  private def req(options: CaseInsensitiveStringMap, name: String): String = {
    val s = options.get(name)
    if (s == null) sys.error(s"$name is required")
    s
  }

  private def opt(options: CaseInsensitiveStringMap, name: String): Option[String] = {
    Option(options.get(name))
  }

  def fromOpts(options: CaseInsensitiveStringMap): HdxConnectionInfo = {
    val orgId = UUID.fromString(req(options, OPT_ORG_ID))
    val url = req(options, OPT_JDBC_URL)
    val user = req(options, OPT_USERNAME)
    val pass = req(options, OPT_PASSWORD)
    val apiUrl = new URI(req(options, OPT_API_URL))
    val partitionPrefix = opt(options, OPT_PARTITION_PREFIX)
    val cloudCred1 = req(options, OPT_CLOUD_CRED_1)
    val cloudCred2 = opt(options, OPT_CLOUD_CRED_2)

    HdxConnectionInfo(orgId, url, user, pass, apiUrl, partitionPrefix, cloudCred1, cloudCred2)
  }
}
