/*
 * Copyright (c) 2023 Hydrolix Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hydrolix.spark.model

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming

import java.net.{URI, URL}
import java.time.Instant
import java.util.UUID

/*
 * These are Scala representations of the JSON schema returned by the Hydrolix API.
 *
 * TODO At the moment they're quite fragile, they break when fields are added or removed.
 *  We should fix that.
 */

case class HdxLoginRequest(username: String,
                           password: String)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxLoginRespAuthToken(accessToken: String,
                                   expiresIn: Long,
                                   tokenType: String)

case class HdxLoginRespOrg(uuid: UUID,
                           name: String,
                         `type`: String,
                          cloud: String,
                     kubernetes: Boolean)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxLoginResponse(uuid: UUID,
                           email: String,
                            orgs: List[HdxLoginRespOrg],
                          groups: List[String],
                       authToken: HdxLoginRespAuthToken)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableStreamSettings(tokenList: List[String],
                       hotDataMaxAgeMinutes: Int,
                 hotDataMaxActivePartitions: Int,
                 hotDataMaxRowsPerPartition: Long,
              hotDataMaxMinutesPerPartition: Long,
                      hotDataMaxOpenSeconds: Long,
                      hotDataMaxIdleSeconds: Long,
                         coldDataMaxAgeDays: Int,
                coldDataMaxActivePartitions: Int,
                coldDataMaxRowsPerPartition: Long,
             coldDataMaxMinutesPerPartition: Int,
                     coldDataMaxOpenSeconds: Int,
                     coldDataMaxIdleSeconds: Int,
                        messageQueueMaxRows: Option[Int])

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsAge(maxAgeDays: Int)
case class HdxTableSettingsMerge(enabled: Boolean,
                                   pools: Map[String, String] = Map(),
                                     sql: Option[String])

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettings(stream: HdxTableStreamSettings,
                               age: HdxTableSettingsAge,
                            reaper: HdxTableSettingsAge,
                             merge: HdxTableSettingsMerge,
                        autoingest: List[HdxTableSettingsAutoIngest],
                          sortKeys: List[String],
                          shardKey: Option[String],
                     maxFutureDays: Int,
                           summary: Option[HdxTableSettingsSummary],
                             scale: Option[HdxTableSettingsScale])

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsSummary(sql: String, enabled: Boolean)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsScale(expectedTbPerDay: Long)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxTableSettingsAutoIngest(enabled: Boolean,
                                       source: String,
                                      pattern: String,
                          maxRowsPerPartition: Long,
                       maxMinutesPerPartition: Long,
                          maxActivePartitions: Int,
                                       dryRun: Boolean,
                                    transform: Option[UUID])

case class HdxApiTable(project: UUID,
                          name: String,
                   description: Option[String],
                          uuid: UUID,
                       created: Instant,
                      modified: Instant,
                      settings: HdxTableSettings,
                           url: URL,
                        `type`: String)

case class HdxProject(uuid: UUID,
                      name: String,
                       org: UUID,
               description: Option[String],
                       url: URI,
                   created: Instant,
                  modified: Instant,
                  settings: HdxProjectSettings)

case class HdxProjectSettings(blob: Option[JsonNode])

case class HdxView(uuid: UUID,
                   name: String,
            description: Option[String],
                created: Instant,
               modified: Instant,
               settings: HdxViewSettings,
                    url: URI,
                  table: UUID)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxViewSettings(isDefault: Boolean,
                       outputColumns: List[HdxOutputColumn])

case class HdxOutputColumn(name: String,
                       datatype: HdxColumnDatatype)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxColumnDatatype(             `type`: HdxValueType,
  @JsonInclude(Include.NON_DEFAULT)        index: Boolean,
  @JsonInclude(Include.NON_DEFAULT)      primary: Boolean,
  @JsonInclude(Include.NON_ABSENT)  indexOptions: Option[JsonNode] = None,
  @JsonInclude(Include.NON_ABSENT)        source: Option[JsonNode] = None,
  @JsonInclude(Include.NON_ABSENT)        format: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)    resolution: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)       default: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)        script: Option[String] = None,
  @JsonInclude(Include.NON_DEFAULT)     catchAll: Boolean = false,
  @JsonInclude(Include.NON_DEFAULT)       ignore: Boolean = false,
  @JsonInclude(Include.NON_ABSENT)      elements: Option[List[HdxColumnDatatype]] = None)

case class HdxStorage(name: String,
                       org: UUID,
               description: Option[String],
                      uuid: UUID,
                       url: URL,
                   created: Instant,
                  modified: Instant,
                  settings: HdxStorageSettings)

@JsonNaming(classOf[SnakeCaseStrategy])
case class HdxStorageSettings(isDefault: Boolean,
                             bucketName: String,
                             bucketPath: String,
                                 region: String,
                                  cloud: String)
