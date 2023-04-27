package io.hydrolix.spark

import model.{HdxLoginRequest, HdxLoginRespAuthToken, HdxLoginResponse, HdxOutputColumn, HdxProject, HdxApiTable, HdxView}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, Expiry, LoadingCache}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}

import java.net.http.HttpRequest.BodyPublishers
import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.UUID

class HdxApiSession(info: HdxConnectionInfo) {
  def tables(db: String): List[HdxApiTable] = {
    val project = database(db).getOrElse(throw NoSuchDatabaseException(db))
    allTablesCache.get(project.uuid)
  }

  def databases(): List[HdxProject] = {
    allProjectsCache.get(0)
  }

  private def database(db: String): Option[HdxProject] = {
    databases().findSingle(_.name == db)
  }

  def table(db: String, table: String): Option[HdxApiTable] = {
    val project = database(db).getOrElse(throw NoSuchDatabaseException(db))
    allTablesCache.get(project.uuid).findSingle(_.name == table)
  }

  private def views(db: String, table: String): List[HdxView] = {
    val tbl = this.table(db, table).getOrElse(throw new NoSuchTableException(table))
    allViewsByTableCache.get(tbl.project -> tbl.uuid)
  }

  def pk(db: String, table: String): HdxOutputColumn = {
    val vs = views(db, table)
    val pkCandidates = vs.filter(_.settings.isDefault).flatMap { view =>
      view.settings.outputColumns.find { col =>
        col.datatype.primary && (col.datatype.`type` == "datetime" || col.datatype.`type` == "datetime64")
      }
    }

    pkCandidates match {
      case List(one) => one
      case Nil => sys.error(s"Couldn't find a primary key for $db.$table")
      case other => sys.error(s"Found multiple candidate primary keys for $db.$table")
    }
  }

  private val client = HttpClient.newHttpClient()

  // It's a bit silly to have a one-element cache here, but we want the auto-renewal
  // TODO this is Integer for stupid Scala 2.12 reasons; it should be Unit. Always pass 0!
  private val tokenCache: LoadingCache[Integer, HdxLoginRespAuthToken] = {
    Caffeine.newBuilder()
      .expireAfter(new Expiry[Integer, HdxLoginRespAuthToken]() {
        private def when(value: HdxLoginRespAuthToken): Long = {
          System.currentTimeMillis() + value.expiresIn - 600 // Renew 10 minutes before expiry
        }

        override def expireAfterCreate(key: Integer,
                                       value: HdxLoginRespAuthToken,
                                       currentTime: Long): Long =
          when(value)

        override def expireAfterUpdate(key: Integer,
                                       value: HdxLoginRespAuthToken,
                                       currentTime: Long,
                                       currentDuration: Long): Long =
          when(value)

        override def expireAfterRead(key: Integer, value: HdxLoginRespAuthToken, currentTime: Long, currentDuration: Long): Long =
          Long.MaxValue
      })
      .build[Integer, HdxLoginRespAuthToken]((_: Integer) => {
        val loginPost = HttpRequest
          .newBuilder(info.apiUrl.resolve("login"))
          .headers("Content-Type", "application/json")
          .POST(BodyPublishers.ofString(JSON.objectMapper.writeValueAsString(HdxLoginRequest(info.user, info.password))))
          .build()

        val loginResp = client.send(loginPost, BodyHandlers.ofString())
        if (loginResp.statusCode() != 200) sys.error(s"POST /login response code was ${loginResp.statusCode()}")

        val loginRespBody = JSON.objectMapper.readValue[HdxLoginResponse](loginResp.body())
        loginRespBody.authToken
      })
  }

  // It's a bit silly to have a one-element cache here, but there's no backend API to find projects by name
  // TODO this is Integer for stupid Scala 2.12 reasons; it should be Unit. Always pass 0!
  private val allProjectsCache: LoadingCache[Integer, List[HdxProject]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build((_: Integer) => {
        val projectGet = HttpRequest
          .newBuilder(info.apiUrl.resolve(s"orgs/${info.orgId}/projects/"))
          .headers("Authorization", s"Bearer ${tokenCache.get(0).accessToken}")
          .GET()
          .build()

        val projectResp = client.send(projectGet, BodyHandlers.ofString())
        if (projectResp.statusCode() != 200) sys.error(s"GET /orgs/:org_id/projects/:project_id response code was ${projectResp.statusCode()}")

        JSON.objectMapper.readValue[List[HdxProject]](projectResp.body())
      })
  }

  private val allTablesCache: LoadingCache[UUID, List[HdxApiTable]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build((key: UUID) => {
        val project = allProjectsCache.get(0).find(_.uuid == key).getOrElse(throw NoSuchDatabaseException(key.toString))

        val tablesGet = HttpRequest
          .newBuilder(info.apiUrl.resolve(s"orgs/${info.orgId}/projects/${project.uuid}/tables/"))
          .headers("Authorization", s"Bearer ${tokenCache.get(0).accessToken}")
          .GET()
          .build()

        val tablesResp = client.send(tablesGet, BodyHandlers.ofString())
        if (tablesResp.statusCode() != 200) sys.error(s"GET /orgs/:org_id/projects/:project_id/tables/ response code was ${tablesResp.statusCode()}")

        JSON.objectMapper.readValue[List[HdxApiTable]](tablesResp.body())
      })
  }

  private val allViewsByTableCache: LoadingCache[(UUID, UUID), List[HdxView]] = {
    Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofHours(1))
      .build[(UUID, UUID), List[HdxView]](new CacheLoader[(UUID, UUID), List[HdxView]]() {
        override def load(key: (UUID, UUID)): List[HdxView] = {
          val (projectId, tableId) = key

          val viewsGet = HttpRequest
            .newBuilder(info.apiUrl.resolve(s"orgs/${info.orgId}/projects/$projectId/tables/$tableId/views/"))
            .headers("Authorization", s"Bearer ${tokenCache.get(0).accessToken}")
            .GET()
            .build()

          val viewsResp = client.send(viewsGet, BodyHandlers.ofString())
          if (viewsResp.statusCode() != 200) sys.error(s"GET /orgs/:org_id/projects/:project_id/tables/:table_id/views/ response code was ${viewsResp.statusCode()}")

          JSON.objectMapper.readValue[List[HdxView]](viewsResp.body())
        }
      })
  }
}
