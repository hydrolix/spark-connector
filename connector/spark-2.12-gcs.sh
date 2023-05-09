#!/usr/bin/env bash

SPARK_HOME="${SPARK_HOME:-$HOME/dev/spark-3.3.2-bin-hadoop3}"
GCS_KEY_PATH="${GCS_KEY_PATH:-$HOME/dev/gcp.key}"
TURBINE_INI_PATH="${TURBINE_INI_PATH:-$HOME/dev/turbine_spark_gcs.ini}"
HDX_API_URL="${HDX_API_URL:-https://gcp-prod-test.hydrolix.net/config/v1/}"
HDX_JDBC_URL="${HDX_JDBC_URL:-jdbc:clickhouse://gcp-prod-test.hydrolix.net:443?ssl=true}"
HDX_ORG_ID="${HDX_ORG_ID:-3287e312-482e-49e1-83fa-11edaa0a66c1}"

if [ -z "$HDX_USER" ]; then
  echo "HDX_USER must be set to your hydrolix cluster username (e.g. alex@hydrolix.io)"
  exit 1
fi

if [ -z "$HDX_PASSWORD" ]; then
  echo "HDX_PASSWORD must be set to your hydrolix cluster password"
  exit 1
fi

if [ ! -e "$GCS_KEY_PATH" ]; then
  echo "GCP service account key file GCS_KEY_PATH=$GCS_KEY_PATH not found"
  exit 1
fi

if [ ! -e "$TURBINE_INI_PATH" ]; then
  echo "turbine.ini file TURBINE_INI_PATH=$TURBINE_INI_PATH not found"
  exit 1
fi

gcpKeyBase64=$(gzip < "$GCS_KEY_PATH" |base64 -w0)
turbineIniBase64=$(gzip < "$TURBINE_INI_PATH" |base64 -w0)

"$SPARK_HOME"/bin/spark-shell \
        --jars ./target/scala-2.12/connector-assembly-0.1.0-SNAPSHOT.jar \
        -c spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
        -c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.HdxTableCatalog \
        -c spark.sql.catalog.hydrolix.storage_type=gcs \
        -c spark.sql.catalog.hydrolix.org_id="$HDX_ORG_ID" \
        -c spark.sql.catalog.hydrolix.jdbc_url="$HDX_JDBC_URL" \
        -c spark.sql.catalog.hydrolix.username="$HDX_USER" \
        -c spark.sql.catalog.hydrolix.password="$HDX_PASSWORD" \
        -c spark.sql.catalog.hydrolix.api_url="$HDX_API_URL" \
        -c spark.sql.catalog.hydrolix.turbine_ini_base64="$turbineIniBase64" \
        -c spark.sql.catalog.hydrolix.cloud_cred_1="$gcpKeyBase64"
        # Add this if you have an old turbine server that doesn't prepend `db/hdx` to the partition paths
	      # -c spark.sql.catalog.hydrolix.partition_prefix=/db/hdx/
