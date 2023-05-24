#!/usr/bin/env bash

SPARK_HOME="${SPARK_HOME:-$HOME/dev/spark-3.3.2-bin-hadoop3}"
HDX_API_URL="${HDX_API_URL:-https://alex-test.hydro59.com/config/v1/}"
HDX_JDBC_URL="${HDX_JDBC_URL:-jdbc:clickhouse://alex-test.hydro59.com:8088/?ssl=true}"
HDX_ORG_ID="${HDX_ORG_ID:-759ca9ab-cc10-41ad-92db-b2710d2e152d}"

if [ -z "$HDX_USER" ]; then
  echo "HDX_USER must be set to your hydrolix cluster username (e.g. alex@hydrolix.io)"
  exit 1
fi

if [ -z "$HDX_PASSWORD" ]; then
  echo "HDX_PASSWORD must be set to your hydrolix cluster password"
  exit 1
fi

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "AWS_ACCESS_KEY_ID must be set -- we need to put it in turbine.ini, context creds aren't enough"
  exit 1
fi

if [ -z "$AWS_SECRET_KEY" ]; then
  echo "AWS_SECRET_KEY must be set -- we need to put it in turbine.ini, context creds aren't enough"
  exit 1
fi

"$SPARK_HOME"/bin/spark-shell \
        --jars ./target/scala-2.12/connector-assembly-0.9.0-SNAPSHOT.jar \
        -c spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
        -c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.HdxTableCatalog \
        -c spark.sql.catalog.hydrolix.storage_type=aws \
        -c spark.sql.catalog.hydrolix.org_id="$HDX_ORG_ID" \
        -c spark.sql.catalog.hydrolix.jdbc_url="$HDX_JDBC_URL" \
        -c spark.sql.catalog.hydrolix.username="$HDX_USER" \
        -c spark.sql.catalog.hydrolix.password="$HDX_PASSWORD" \
        -c spark.sql.catalog.hydrolix.api_url="$HDX_API_URL" \
        -c spark.sql.catalog.hydrolix.cloud_cred_1="$AWS_ACCESS_KEY_ID" \
        -c spark.sql.catalog.hydrolix.cloud_cred_2="$AWS_SECRET_KEY"