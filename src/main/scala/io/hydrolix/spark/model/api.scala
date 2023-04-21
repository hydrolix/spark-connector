package io.hydrolix.spark
package model

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
 *  We should fix that before this ships.
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
                      coldDataMaxIdleSeconds: Int)

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

case class HdxColumnDatatype(`type`: String,
                              index: Boolean,
                            primary: Boolean,
                             source: Option[JsonNode],
                             format: Option[String],
                         resolution: Option[String],
                            default: Option[String],
                             script: Option[String],
                           elements: Option[JsonNode])