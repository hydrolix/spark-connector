package io.hydrolix.spark.model

import com.fasterxml.jackson.annotation.{JsonInclude, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{JsonNode, PropertyNamingStrategies}
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

case class HdxColumnDatatype(          `type`: HdxValueType,
  @JsonInclude(Include.NON_DEFAULT)     index: Boolean,
  @JsonInclude(Include.NON_DEFAULT)   primary: Boolean,
  @JsonInclude(Include.NON_ABSENT)     source: Option[JsonNode] = None,
  @JsonInclude(Include.NON_ABSENT)     format: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT) resolution: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)    default: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)     script: Option[String] = None,
  @JsonInclude(Include.NON_ABSENT)   elements: Option[JsonNode] = None)

/**
 * TODO sync this with the Elastic project somehow:
 *  https://gitlab.com/hydrolix/interop-kibana/-/blob/main/hydrolix-specific/src/main/kotlin/io/hydrolix/ketchup/model/hdx/Transform.kt
 */
case class HdxTransform(name: String,
                 description: Option[String],
                      `type`: HdxTransformType,
                       table: String,
                    settings: HdxTransformSettings)


@JsonNaming(classOf[PropertyNamingStrategies.SnakeCaseStrategy])
case class HdxTransformSettings(isDefault: Boolean,
                              compression: HdxTransformCompression, // TODO layering
                             sqlTransform: Option[String] = None,
                               nullValues: List[String] = Nil,
                            formatDetails: HdxTransformFormatDetails,
                            outputColumns: List[HdxOutputColumn])


@JsonSubTypes(value = Array(
  new JsonSubTypes.Type(name = "csv", value = classOf[CsvFormatDetails]),
  new JsonSubTypes.Type(name = "json", value = classOf[JsonFormatDetails]),
))
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "type")
trait HdxTransformFormatDetails {}

@JsonNaming(classOf[PropertyNamingStrategies.SnakeCaseStrategy])
case class CsvFormatDetails(delimiter: String, // TODO can be a number too?
                               escape: String, // TODO number
                             skipHead: Option[Int],
                                quote: Option[String], // TODO number
                              comment: Option[String], // TODO number
                         skipComments: Boolean,
                        windowsEnding: Boolean)
  extends HdxTransformFormatDetails


case class JsonFormatDetails(flattening: JsonFlattening) extends HdxTransformFormatDetails

@JsonNaming(classOf[PropertyNamingStrategies.SnakeCaseStrategy])
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
case class JsonFlattening(active: Boolean,
           mapFlatteningStrategy: Option[FlatteningStrategy],
         sliceFlatteningStrategy: Option[FlatteningStrategy],
                           depth: Option[Int])

case class FlatteningStrategy(left: String,
                             right: String)
