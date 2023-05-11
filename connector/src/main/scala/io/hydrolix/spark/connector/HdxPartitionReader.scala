package io.hydrolix.spark.connector

import io.hydrolix.spark.model._

import com.google.common.io.ByteStreams
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, File, FileOutputStream}
import java.util.Base64
import java.util.concurrent.ArrayBlockingQueue
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import scala.collection.mutable
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Using.resource
import scala.util.{Try, Using}

/**
 * This is due to https://bugs.openjdk.org/browse/JDK-8068370 -- we spawn child processes too fast and they
 * accidentally clobber each other's temp files
 */
object HdxPartitionReader {
  private val logger = LoggerFactory.getLogger(classOf[HdxPartitionReaderFactory])

  // TODO try not to recreate these files every time if they're unchanged... Maybe name them according to a git hash
  //  or a sha256sum of the contents?
  private val turbineCmdTmp = {
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

  private val stderrFilterR = """^read hdx_partition=(.*?) rows=(.*?) values=(.*?) in (.*?)$""".r
  // Note, these regexes can break if turbine.ini format changes!
  private val gcsCredentialsR = Pattern.compile("""^(\s*fs.gcs.credentials.json_credentials_file\s*=\s*)(.*?)\s*$""", Pattern.MULTILINE)
  private val awsMethodR = Pattern.compile("""^(\s*fs.aws.credentials.method\s*=\s*)(.*?)\s*$""", Pattern.MULTILINE)
  private val awsAccessKeyR = Pattern.compile("""^(\s*fs.aws.credentials.access_key\s*=\s*)(.*?)\s*$""", Pattern.MULTILINE)
  private val awsSecretKeyR = Pattern.compile("""^(\s*fs.aws.credentials.secret_key\s*=\s*)(.*?)\s*$""", Pattern.MULTILINE)
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
                     primaryKeyName: String,
                               scan: HdxScanPartition)
  extends PartitionReader[InternalRow]
    with Logging
{
  import HdxPartitionReader._

  private val doneSignal = "DONE"
  private val deadSignal = "DEAD"
  private val q = new ArrayBlockingQueue[String](1024)
  @volatile private var exitCode: Option[Int] = None

  // Cache because PartitionReader says get() should always return the same record if called multiple times per next()
  private var rec: InternalRow = _

  private val schema = scan.schema.fields.map { fld =>
    HdxOutputColumn(fld.name, Types.sparkToHdx(fld.name, fld.dataType, primaryKeyName, scan.hdxCols))
  }

  private val schemaStr = JSON.objectMapper.writeValueAsString(schema)

  private val exprArgs = {
    // TODO Spark seems to inject a `foo IS NOT NULL` alongside a `foo = <lit>`, maybe filter it out before doing this

    val renderedPreds = scan.pushed.map(HdxPushdown.renderHdxFilterExpr(_, primaryKeyName, scan.hdxCols))
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    // TODO this assumes it's safe to push down partial predicates (as long as they're an AND?), double check!
    val expr = renderedPreds.flatten.mkString("[", " AND ", "]")
    log.info(s"Pushed-down expression: $expr")
    if (renderedPreds.isEmpty) Nil else List("--expr", expr)
  }

  private val turbineIniBefore = resource(
    new GZIPInputStream(new ByteArrayInputStream(
      Base64.getDecoder.decode(info.turbineIniBase64)
    ))
  ) { is =>
    new String(ByteStreams.toByteArray(is), "UTF-8")
  }

  private val (turbineIniAfter, credsTempFile) = if (info.storageType == "gcs") {
    val gcsKeyFile = File.createTempFile("turbine_gcs_key_", ".json")
    gcsKeyFile.deleteOnExit()

    val turbineIni = Using.Manager { use =>
      // For gcs, cloudCred1 is a base64(gzip(gcs_service_account_key.json)) and cloudCred2 is unused
      val gcsKeyB64 = Base64.getDecoder.decode(info.cloudCred1)

      val gcsKeyBytes = ByteStreams.toByteArray(use(new GZIPInputStream(new ByteArrayInputStream(gcsKeyB64))))
      use(new FileOutputStream(gcsKeyFile)).write(gcsKeyBytes)

      val turbineIniWithGcsCredsPath = gcsCredentialsR.matcher(turbineIniBefore).replaceAll(s"$$1${gcsKeyFile.getAbsolutePath}")

      turbineIniWithGcsCredsPath
    }.get

    (turbineIni, Some(gcsKeyFile))
  } else if (info.storageType == "aws") {
    // For aws, cloudCred1 is the access key, and cloudCred2 is the secret.
    val accessKey = info.cloudCred1
    val secret = info.cloudCred2.getOrElse(sys.error("cloud_cred_2 is required for aws!"))

    // TODO other AWS settings like region need a place to live too; for now pre-populate the turbine.ini
    val s2 = awsMethodR.matcher(turbineIniBefore).replaceAll(s"$$1static")
    val s3 = awsAccessKeyR.matcher(s2).replaceAll(s"$$1$accessKey")
    val s4 = awsSecretKeyR.matcher(s3).replaceAll(s"$$1$secret")

    (s4, None)
  } else {
    // TODO implement other storage types, but using turbine.ini unchanged is a good fallback
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
    "--output_format", "json",
    "--hdx_partition", s"${info.partitionPrefix.getOrElse("")}${scan.path}",
    "--output_path", "-",
    "--schema", schemaStr
  ) ++ exprArgs
  log.info(s"Running ${turbineCmdTmp.getAbsolutePath} ${turbineCmdArgs.mkString(" ")}")

  private val hdxReaderProcessBuilder = Process(turbineCmdTmp.getAbsolutePath, turbineCmdArgs)
  private val stderr = mutable.ListBuffer[String]()

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
    {
      case stderrFilterR(_*) => ()
      case line => stderr += line
    }
  ))

  // A dumb thread to wait for the child to exit so we can send doneSignal on the queue, and capture the exit code
  new Thread(() => {
    try {
      val exit = hdxReaderProcess.exitValue() // Blocks until exit
      exitCode = Some(exit)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    } finally {
      if (exitCode.contains(0)) {
        q.put(doneSignal)
      } else {
        q.put(deadSignal)
      }
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
    } else if (line eq deadSignal) {
      throw new RuntimeException(s"turbine_cmd process exited with code ${exitCode.getOrElse("<UNKNOWN, this is a bug!>")}; stderr was: ${stderr.mkString("\n  ", "\n  ", "\n")}")
    } else {
      rec = Json2Row.row(scan.schema, line)
      true
    }
  }

  override def get(): InternalRow = rec

  override def close(): Unit = {
    Try(turbineIniTmp.delete())
    credsTempFile.foreach(f => Try(f.delete()))

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
