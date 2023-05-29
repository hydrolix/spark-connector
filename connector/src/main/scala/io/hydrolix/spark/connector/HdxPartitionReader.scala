package io.hydrolix.spark.connector

import io.hydrolix.spark.model._

import com.google.common.io.ByteStreams
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.slf4j.LoggerFactory

import java.io._
import java.util.Base64
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.GZIPInputStream
import scala.collection.mutable
import scala.sys.process.{Process, ProcessIO}
import scala.util.Using.resource
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Try, Using}

class HdxPartitionReaderFactory(info: HdxConnectionInfo, storage: HdxStorageSettings, pkName: String)
  extends PartitionReaderFactory
{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = sys.error("No row-oriented anymore")

  override def supportColumnarReads(partition: InputPartition) = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new HdxPartitionReader(info, storage, pkName, partition.asInstanceOf[HdxScanPartition])
  }
}

object HdxPartitionReader {
  private val logger = LoggerFactory.getLogger(classOf[HdxPartitionReader])

  /**
   * This is done early before any work needs to be done because of https://bugs.openjdk.org/browse/JDK-8068370 -- we
   * spawn child processes too fast and they accidentally clobber each other's temp files
   *
   * TODO try not to recreate these files every time if they're unchanged... Maybe name them according to a git hash
   *  or a sha256sum of the contents?
   */
  private lazy val turbineCmdTmp = {
    val f = File.createTempFile("turbine_cmd_", ".exe")
    f.deleteOnExit()

    Using.Manager { use =>
      ByteStreams.copy(
        use(getClass.getResourceAsStream("/turbine_cmd")),
        use(new FileOutputStream(f))
      )
    }.get

    f.setExecutable(true)

    logger.info(s"Extracted turbine_cmd binary to ${f.getAbsolutePath}")

    f
  }

  private def readLines(stream: InputStream, onLine: String => Unit, onDone: => Unit, bufSize: Int = 16384): Unit = {
    // TODO GZIP goes here if we ever get that working in turbine_cmd
    Using.resource(new BufferedReader(new InputStreamReader(stream, "UTF-8"), bufSize)) { reader =>
      breakable {
        while (true) {
          val line = reader.readLine()
          try {
            if (line == null) {
              onDone
              break()
            } else {
              onLine(line)
            }
          } catch {
            case _: InterruptedException =>
              // If we got killed early (e.g. because of a LIMIT) let's be quiet about it
              Thread.currentThread().interrupt()
              break()
          }
        }
      }
    }
  }

  private val stderrFilterR = """^read hdx_partition=(.*?) rows=(.*?) values=(.*?) in (.*?)$""".r

  private val doneSignal = new ColumnarBatch(Array())
}

/**
 * TODO:
 *  - Refactor per-storage-type turbine.ini hacking and temp files to separate classes
 *  - Make it possible to retrieve the turbine.ini template from a URL instead of passing a large base64 blob
 *  - Refactor the child process stuff to separate file with a clean module boundary
 *  - Allow secrets to be retrieved from secret services, not just config parameters
 *  - Make a ColumnarBatch version too (will require deep hdx_reader changes)
 */
final class HdxPartitionReader(info: HdxConnectionInfo,
                            storage: HdxStorageSettings,
                     primaryKeyName: String,
                               scan: HdxScanPartition)
  extends PartitionReader[ColumnarBatch]
    with Logging
{
  import HdxPartitionReader._

  private val schema = scan.schema.fields.map { fld =>
    HdxOutputColumn(fld.name, scan.hdxCols.getOrElse(fld.name, sys.error(s"No HdxColumnInfo for ${fld.name}")).hdxType)
  }

  private val schemaStr = JSON.objectMapper.writeValueAsString(schema)

  private val exprArgs = {
    // TODO Spark seems to inject a `foo IS NOT NULL` alongside a `foo = <lit>`, maybe filter it out before doing this

    val renderedPreds = scan.pushed.flatMap(HdxPushdown.renderHdxFilterExpr(_, primaryKeyName, scan.hdxCols))
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    val expr = renderedPreds.mkString("[", " AND ", "]")
    if (renderedPreds.isEmpty) Nil else List("--expr", expr)
  }

  private val turbineIniBefore = TurbineIni(storage, info.cloudCred1, info.cloudCred2)

  private val (turbineIniAfter, credsTempFile) = if (storage.cloud == "gcp") {
    val gcsKeyFile = File.createTempFile("turbine_gcs_key_", ".json")
    gcsKeyFile.deleteOnExit()

    val turbineIni = Using.Manager { use =>
      // For gcs, cloudCred1 is a base64(gzip(gcs_service_account_key.json)) and cloudCred2 is unused
      val gcsKeyB64 = Base64.getDecoder.decode(info.cloudCred1)

      val gcsKeyBytes = ByteStreams.toByteArray(use(new GZIPInputStream(new ByteArrayInputStream(gcsKeyB64))))
      use(new FileOutputStream(gcsKeyFile)).write(gcsKeyBytes)

      val turbineIniWithGcsCredsPath = turbineIniBefore.replace("%CREDS_FILE%", gcsKeyFile.getAbsolutePath)

      turbineIniWithGcsCredsPath
    }.get

    (turbineIni, Some(gcsKeyFile))
  } else {
    // AWS doesn't need any further munging of turbine.ini
    (turbineIniBefore, None)
  }

  private val turbineIniTmp = File.createTempFile("turbine_ini_", ".ini")
  turbineIniTmp.deleteOnExit()
  resource(new FileOutputStream(turbineIniTmp)) { _.write(turbineIniAfter.getBytes("UTF-8")) }

  // TODO does anything need to be quoted here?
  //  Note, this relies on a bunch of changes in hdx_reader that may not have been merged to turbine/turbine-core yet,
  //  see https://hydrolix.atlassian.net/browse/HDX-3779
  private val turbineCmdArgs = List(
    "hdx_reader",
    "--config", turbineIniTmp.getAbsolutePath,
    "--output_format", "jsonc",
    "--hdx_partition", s"${info.partitionPrefix.getOrElse("")}${scan.path}",
    "--output_path", "-",
    "--schema", schemaStr
  ) ++ exprArgs
  log.info(s"Running ${turbineCmdTmp.getAbsolutePath} ${turbineCmdArgs.mkString(" ")}")

  private val counter = new AtomicLong(1) // 1 => make room for doneSignal
  private val batchQ = new ArrayBlockingQueue[ColumnarBatch](100)

  // Cache because PartitionReader says get() should always return the same record if called multiple times per next()
  @volatile private var batch: ColumnarBatch = _

  private val hdxReaderProcessBuilder = Process(turbineCmdTmp.getAbsolutePath, turbineCmdArgs)
  private val capturedStderr = mutable.ListBuffer[String]()

  // TODO this relies on the stdout being split into strings; that won't be the case once we get gzip working!
  private val hdxReaderProcess = hdxReaderProcessBuilder.run(new ProcessIO(
    { _.close() }, // Don't care about stdin
    { stdout =>
      HdxReaderColumnarJson.batches(scan.schema, stdout, batchQ.put, batchQ.put(doneSignal))
    },
    readLines(_,
      {
        case stderrFilterR(_*) => () // Ignore expected output
        case l => capturedStderr += l // Capture unexpected output
      },
      () // No need to do anything special when stderr drains
    )
  ))

  override def next(): Boolean = {
    val batch = try {
      batchQ.take() // It's OK to block here, we'll always have doneSignal...
    } catch {
      case _: InterruptedException =>
        // ...But if we got killed while waiting, don't be noisy about it
        Thread.currentThread().interrupt()
        return false
    }

    if (batch eq doneSignal) {
      // stdout is closed, now we can wait for the sweet release of death
      val exit = hdxReaderProcess.exitValue()
      if (exit != 0) {
        throw new RuntimeException(s"turbine_cmd process exited with code $exit; stderr was: ${capturedStderr.mkString("\n  ", "\n  ", "\n")}")
      } else {
        // There are definitely no more records
        false
      }
    } else {
      this.batch = batch

      counter.decrementAndGet() > 0
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    Try(turbineIniTmp.delete())
    credsTempFile.foreach(f => Try(f.delete()))
  }
}
