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
package io.hydrolix.spark.connector

import io.hydrolix.spark.model._

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, Expiry, LoadingCache}
import org.apache.hc.client5.http.HttpResponseException
import org.apache.hc.client5.http.classic.methods.{HttpGet, HttpPost, HttpUriRequest}
import org.apache.hc.client5.http.impl.classic.{BasicHttpClientResponseHandler, HttpClients}
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}

import java.time.Duration
import java.util.UUID

final class HdxApiSession(info: HdxConnectionInfo) {
  def tables(db: String): List[HdxApiTable] = {
    val project = database(db).getOrElse(throw NoSuchDatabaseException(db))
    allTablesCache.get(project.uuid)
  }

  def databases(): List[HdxProject] = {
    allProjectsCache.get(0)
  }

  def storages(): List[HdxStorage] = {
    allStoragesCache.get(0)
  }

  private def database(db: String): Option[HdxProject] = {
    // This is where a DB name collision in multiple orgs would fail
    databases().findSingle(_.name == db)
  }

  def table(db: String, table: String): Option[HdxApiTable] = {
    val project = database(db).getOrElse(throw NoSuchDatabaseException(db))
    allTablesCache.get(project.uuid).findSingle(_.name == table)
  }

  def views(db: String, table: String): List[HdxView] = {
    val tbl = this.table(db, table).getOrElse(throw NoSuchTableException(table))
    allViewsByTableCache.get(tbl.project -> tbl.uuid)
  }

  def defaultView(db: String, table: String): HdxView = {
    val vs = views(db, table)
    vs.findExactlyOne(_.settings.isDefault, "default view")
  }

  def pk(db: String, table: String): HdxOutputColumn = {
    val vs = views(db, table)
    val pkCandidates = vs.filter(_.settings.isDefault).flatMap { view =>
      view.settings.outputColumns.find { col =>
        col.datatype.primary && (col.datatype.`type` == HdxValueType.DateTime || col.datatype.`type` == HdxValueType.DateTime64)
      }
    }

    pkCandidates match {
      case List(one) => one
      case Nil => sys.error(s"Couldn't find a primary key for $db.$table")
      case other => sys.error(s"Found multiple candidate primary keys for $db.$table: ${other.mkString(", ")}")
    }
  }

  def version(): String = {
    val get = new HttpGet(info.apiUrl.resolve("/version")).withAuthToken()
    client.execute(get, new BasicHttpClientResponseHandler())
  }

  private val client = HttpClients.createDefault()

  // It's a bit silly to have a one-element cache here, but we want the auto-renewal
  // TODO this is Integer for stupid Scala 2.12 reasons; it should be Unit. Always pass 0!
  private val authRespCache: LoadingCache[Integer, HdxLoginResponse] = {
    Caffeine.newBuilder()
      .expireAfter(new Expiry[Integer, HdxLoginResponse]() {
        private def when(value: HdxLoginResponse): Long = {
          System.currentTimeMillis() + value.authToken.expiresIn - 600 // Renew 10 minutes before expiry
        }

        override def expireAfterCreate(key: Integer,
                                       value: HdxLoginResponse,
                                       currentTime: Long): Long =
          when(value)

        override def expireAfterUpdate(key: Integer,
                                       value: HdxLoginResponse,
                                       currentTime: Long,
                                       currentDuration: Long): Long =
          when(value)

        override def expireAfterRead(key: Integer, value: HdxLoginResponse, currentTime: Long, currentDuration: Long): Long =
          Long.MaxValue
      })
      .build((_: Integer) => {
        val loginPost = new HttpPost(info.apiUrl.resolve("login"))
        loginPost.setEntity(new StringEntity(
          JSON.objectMapper.writeValueAsString(HdxLoginRequest(info.user, info.password)),
          ContentType.APPLICATION_JSON
        ))
        val loginResp = client.execute(loginPost, new BasicHttpClientResponseHandler())

        JSON.objectMapper.readValue[HdxLoginResponse](loginResp)
      })
  }

  // It's a bit silly to have a one-element cache here, but there's no backend API to find projects by name
  // TODO this is Integer for stupid Scala 2.12 reasons; it should be Unit. Always pass 0!
  private val allProjectsCache: LoadingCache[Integer, List[HdxProject]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build((_: Integer) => {
        val orgIds = authRespCache.get(0).orgs.map(_.uuid).toSet

        orgIds.toList.flatMap { orgId =>
          val projectsGet = new HttpGet(info.apiUrl.resolve(s"orgs/$orgId/projects/"))
            .withAuthToken()

          try {
            val projectsResp = client.execute(projectsGet, new BasicHttpClientResponseHandler())
            JSON.objectMapper.readValue[List[HdxProject]](projectsResp)
          } catch {
            case e: HttpResponseException if e.getStatusCode == 404 => Nil
          }
        }
      })
  }

  implicit class HttpRequestGoodies(req: HttpUriRequest) {
    def withAuthToken(): HttpUriRequest = {
      req.addHeader("Authorization", s"Bearer ${authRespCache.get(0).authToken.accessToken}")
      req
    }
  }

  private val allTablesCache: LoadingCache[UUID, List[HdxApiTable]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build((key: UUID) => {
        val orgIds = authRespCache.get(0).orgs.map(_.uuid).toSet

        orgIds.toList.flatMap { orgId =>
          val project = allProjectsCache.get(0).find(_.uuid == key).getOrElse(throw NoSuchDatabaseException(key.toString))

          val tablesGet = new HttpGet(info.apiUrl.resolve(s"orgs/$orgId/projects/${project.uuid}/tables/"))
            .withAuthToken()

          try {
            JSON.objectMapper.readValue[List[HdxApiTable]](client.execute(tablesGet, new BasicHttpClientResponseHandler()))
          } catch {
            case e: HttpResponseException if e.getStatusCode == 404 => Nil
          }
        }
      })
  }

  private val allStoragesCache: LoadingCache[Integer, List[HdxStorage]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build((_: Integer) => {
        val orgIds = authRespCache.get(0).orgs.map(_.uuid).toSet
        orgIds.toList.flatMap { orgId =>
          val storagesGet = new HttpGet(info.apiUrl.resolve(s"orgs/$orgId/storages/"))
            .withAuthToken()

          try {
            val storagesResp = client.execute(storagesGet, new BasicHttpClientResponseHandler())
            JSON.objectMapper.readValue[List[HdxStorage]](storagesResp)
          } catch {
            case e: HttpResponseException if e.getStatusCode == 404 => Nil
          }
        }
      })
  }

  private val allViewsByTableCache: LoadingCache[(UUID, UUID), List[HdxView]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build[(UUID, UUID), List[HdxView]](new CacheLoader[(UUID, UUID), List[HdxView]]() {
        override def load(key: (UUID, UUID)): List[HdxView] = {
          val (projectId, tableId) = key
          val orgIds = authRespCache.get(0).orgs.map(_.uuid).toSet
          orgIds.toList.flatMap { orgId =>
            val viewsGet = new HttpGet(info.apiUrl.resolve(s"orgs/$orgId/projects/$projectId/tables/$tableId/views/"))
              .withAuthToken()

            try {
              val viewsResp = client.execute(viewsGet, new BasicHttpClientResponseHandler())
              JSON.objectMapper.readValue[List[HdxView]](viewsResp)
            } catch {
              case e: HttpResponseException if e.getStatusCode == 404 => Nil
            }
          }
        }
      })
  }
}
