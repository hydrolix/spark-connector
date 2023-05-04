package io.hydrolix.spark.connector

import io.hydrolix.spark.model._

import com.google.common.io.ByteStreams
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.util.Base64
import java.util.concurrent.ArrayBlockingQueue
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Using.resource
import scala.util.{Try, Using}

// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
class HdxPartitionReader(info: HdxConnectionInfo,
               primaryKeyName: String,
                         scan: HdxPartitionScan)
  extends PartitionReader[InternalRow]
    with Logging
{
  private val doneSignal = "DONE"
  private val q = new ArrayBlockingQueue[String](1024)
  @volatile private var exitCode: Option[Int] = None

  // Cache because PartitionReader says get() should always return the same record if called multiple times per next()
  private var rec: InternalRow = _

  private val cols = HdxJdbcSession(info).collectColumns(scan.db, scan.table).map(col => col.name -> col).toMap
  private val schema = scan.schema.fields.map { fld =>
    HdxOutputColumn(fld.name, Types.sparkToHdx(fld.name, fld.dataType, primaryKeyName, cols))
  }

  private val schemaStr = JSON.objectMapper.writeValueAsString(schema)

  private val exprArgs = {
    // TODO Spark seems to inject a `foo IS NOT NULL` alongside a `foo = <lit>`, maybe filter it out before doing this

    val renderedPushed = scan.pushed.map(HdxPredicatePushdown.renderHdxFilterExpr(_, primaryKeyName, cols))
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    if (renderedPushed.isEmpty) Nil else List("--expr", renderedPushed.flatten.mkString("[", " AND ", "]"))
  }

  // TODO try not to recreate these files every time (if they're unchanged?)
  private val turbineCmdTmp = File.createTempFile("turbine_cmd_", ".exe")
  turbineCmdTmp.deleteOnExit()
  turbineCmdTmp.setExecutable(true)
  Using.Manager { use =>
    ByteStreams.copy(use(getClass.getResourceAsStream("/turbine_cmd")), use(new FileOutputStream(turbineCmdTmp)))
  }.get

  private val turbineIniTmp = File.createTempFile("turbine_ini_", ".ini")
  turbineIniTmp.deleteOnExit()

  private val turbineIniBytes = resource(new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(info.turbineIniBase64)))) { ByteStreams.toByteArray(_) }
  // Note this regex can break if turbine.ini format changes!
  private val gcsCredentialsR = Pattern.compile("""^(\s*fs.gcs.credentials.json_credentials_file\s*=\s*)(.*?)\s*$""", Pattern.MULTILINE)

  private val credTmp = if (info.storageType == "gcs") {
    val gcsKeyFile = File.createTempFile("turbine_gcs_key_", ".json")
    gcsKeyFile.deleteOnExit()
    Using.Manager { use =>
      // For gcs, cloudCred1 is a base64(gzip(gcs_service_account_key.json))
      val gcsKeyB64 = Base64.getDecoder.decode(info.cloudCred1)

      val gcsKeyBytes = ByteStreams.toByteArray(use(new GZIPInputStream(new ByteArrayInputStream(gcsKeyB64))))

      val s = new String(turbineIniBytes, "UTF-8")
      val s2 = gcsCredentialsR.matcher(s).replaceAll(s"$$1${gcsKeyFile.getAbsolutePath}")

      use(new FileOutputStream(turbineIniTmp)).write(s2.getBytes("UTF-8"))
      use(new FileOutputStream(gcsKeyFile)).write(gcsKeyBytes)
    }.get

    Some(gcsKeyFile)
  } else {
    // TODO replace the appropriate bits of turbine.ini for other storageTypes too
    Using.resource(new FileOutputStream(turbineIniTmp)) { _.write(turbineIniBytes) }
    None
  }

  // TODO does anything need to be quoted here?
  //  Note, this relies on a bunch of changes in hdx_reader that may not have been merged to turbine/turbine-core yet,
  //  see https://hydrolix.atlassian.net/browse/HDX-3779
  private val turbineCmdArgs = List(
    "hdx_reader",
    "--config", turbineIniTmp.getAbsolutePath,
    "--output_format", "json",
    "--hdx_partition", scan.path,
    "--output_path", "-",
    "--schema", schemaStr
  ) ++ exprArgs

  private val hdxReaderProcessBuilder = Process(turbineCmdTmp.getAbsolutePath, turbineCmdArgs)

  // TODO this relies on the stdout being split into strings; that won't be the case once we get gzip working!
  private val hdxReaderProcess = hdxReaderProcessBuilder.run(ProcessLogger(
    { line =>
      try {
        q.put(line)
      } catch {
        case _: InterruptedException =>
          // If we got killed early (e.g. because of a LIMIT) let's be quiet about it
          Thread.currentThread().interrupt()
      }
    },
    { log.warn("hdx_reader stderr: {}", _) }
  ))

  // A dumb thread to wait for the child to exit so we can send doneSignal on the queue, and capture the exit code
  new Thread(() => {
    try {
      exitCode = Some(hdxReaderProcess.exitValue()) // Blocks until exit
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    } finally {
      q.put(doneSignal)
    }
  }).start()

  override def next(): Boolean = {
    val line = try {
      q.take() // It's OK to block here, we'll always have doneSignal...
    } catch {
      case _: InterruptedException =>
        // ...But if we got killed while waiting, don't be noisy about it
        Thread.currentThread().interrupt()
        return false
    }

    if (line eq doneSignal) {
      false
    } else {
      rec = Json2Row.row(scan.schema, line)
      true
    }
  }

  override def get(): InternalRow = rec

  override def close(): Unit = {
    Try(turbineCmdTmp.delete())
    Try(turbineIniTmp.delete())
    credTmp.foreach(f => Try(f.delete()))

    if (hdxReaderProcess.isAlive()) {
      log.debug(s"Killing child process for partition ${scan.path} early")
      hdxReaderProcess.destroy()
    } else {
      exitCode match {
        case None => log.error("Sanity violation: process exited but we have no exit code!")
        case Some(i) if i != 0 => log.warn(s"Process exited with status $i")
        case Some(_) => // OK
      }
    }
  }
}
