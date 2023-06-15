#!/usr/bin/env bash

# Copyright (c) 2023 Hydrolix Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SPARK_HOME="${SPARK_HOME:-$HOME/dev/spark-3.3.2-bin-hadoop3}"
HDX_API_URL="${HDX_API_URL:-https://hdx-cluster.example.com/config/v1/}"
HDX_JDBC_URL="${HDX_JDBC_URL:-jdbc:clickhouse:tcp://hdx-cluster.example.com:9440/hydro?ssl=true}"
HDX_ORG_ID="${HDX_ORG_ID:-11111111-2222-4333-4444-555555555555}"

if [ -z "$HDX_USER" ]; then
  echo "HDX_USER must be set to your hydrolix cluster username (e.g. alice@example.com)"
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
        --jars ../target/scala-2.12/hydrolix-spark-connector-assembly-1.0.0-SNAPSHOT.jar \
        -c spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
        -c spark.sql.catalog.hydrolix=io.hydrolix.spark.connector.HdxTableCatalog \
        -c spark.sql.catalog.hydrolix.org_id="$HDX_ORG_ID" \
        -c spark.sql.catalog.hydrolix.jdbc_url="$HDX_JDBC_URL" \
        -c spark.sql.catalog.hydrolix.username="$HDX_USER" \
        -c spark.sql.catalog.hydrolix.password="$HDX_PASSWORD" \
        -c spark.sql.catalog.hydrolix.api_url="$HDX_API_URL" \
        -c spark.sql.catalog.hydrolix.cloud_cred_1="$AWS_ACCESS_KEY_ID" \
        -c spark.sql.catalog.hydrolix.cloud_cred_2="$AWS_SECRET_KEY"