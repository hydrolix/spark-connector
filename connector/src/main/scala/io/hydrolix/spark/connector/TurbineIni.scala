package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxStorageSettings

import com.google.common.io.ByteStreams
import org.apache.spark.internal.Logging

import scala.util.Using

object TurbineIni extends Logging {
  private val template = Using.resource(getClass.getResourceAsStream("/turbine_template.ini")) { stream =>
    new String(ByteStreams.toByteArray(stream), "UTF-8")
  }

  def apply(storage: HdxStorageSettings, cloudCred1: String, cloudCred2: Option[String]): String = {
    val (creds, storageInfo) = storage.cloud match {
      case "gcp" | "gcs" =>
        (
          """# GCS credentials
             |fs.gcs.credentials.method = service_account
             |fs.gcs.credentials.json_credentials_file = %CREDS_FILE%
             |""".stripMargin,

          s"""# GCS storage info
              |fs.id.default.type = gcs
              |fs.id.default.gcs.region = ${storage.region}
              |fs.id.default.gcs.storage.bucket_name = ${storage.bucketName}
              |fs.id.default.gcs.storage.bucket_path = ${storage.bucketPath}
              |""".stripMargin
        )

      case "aws" =>
        (
          s"""# AWS credentials
              |fs.aws.credentials.method = static
              |fs.aws.credentials.access_key = $cloudCred1
              |fs.aws.credentials.secret_key = ${cloudCred2.getOrElse(sys.error("cloud_cred_2 is required for AWS"))}
              |""".stripMargin,

          s"""# AWS storage info
              |fs.id.default.type = s3
              |fs.id.default.aws.region = ${storage.region}
              |fs.id.default.aws.s3.bucket_name = ${storage.bucketName}
              |fs.id.default.aws.s3.bucket_path = ${storage.bucketPath}
              |""".stripMargin
        )

      case other =>
        log.warn(s"Don't know how to generate turbine.ini for storage type $other; leaving it blank and hoping for the best")
        (
          s"# unknown credentials for $other",
          s"# unknown storage info $other"
        )
    }

    template
      .replace("%CLOUD_CREDS%", creds)
      .replace("%CLOUD_STORAGE_INFO%", storageInfo)
  }
}
