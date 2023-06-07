#!/usr/bin/env bash

SPARK_HOME="${SPARK_HOME:-$HOME/dev/spark-3.3.2-bin-hadoop3}"
GCS_KEY_PATH="${GCS_KEY_PATH:-$HOME/dev/gcp.key}"
HDX_API_URL="${HDX_API_URL:-https://hdx-cluster.example.com/config/v1/}"
HDX_JDBC_URL="${HDX_JDBC_URL:-jdbc:clickhouse:tcp://hdx-cluster.example.com:9440/hydro?ssl=true}"
HDX_ORG_ID="${HDX_ORG_ID:-11111111-2222-4333-4444-555555555555}"

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

gcpKeyBase64=$(gzip < "$GCS_KEY_PATH" |base64 -w0)

"$SPARK_HOME"/bin/spark-shell \
        --jars ../target/scala-2.12/hydrolix-spark-connector-assembly-1.0.0-SNAPSHOT.jar \
        -c spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
        -c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.HdxTableCatalog \
        -c spark.sql.catalog.hydrolix.org_id="$HDX_ORG_ID" \
        -c spark.sql.catalog.hydrolix.jdbc_url="$HDX_JDBC_URL" \
        -c spark.sql.catalog.hydrolix.username="$HDX_USER" \
        -c spark.sql.catalog.hydrolix.password="$HDX_PASSWORD" \
        -c spark.sql.catalog.hydrolix.api_url="$HDX_API_URL" \
        -c spark.sql.catalog.hydrolix.cloud_cred_1="$gcpKeyBase64"
