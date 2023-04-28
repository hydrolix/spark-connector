package io.hydrolix.spark.connector

import io.hydrolix.spark.model.HdxOutputColumn
import org.apache.spark.internal.Logging
import org.apache.spark.sql.HdxPredicatePushdown
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util.concurrent.ArrayBlockingQueue
import scala.sys.process.{Process, ProcessLogger}

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

  // TODO does anything need to be quoted here?
  //  Note, this relies on a bunch of changes in hdx_reader that may not have been merged to turbine/turbine-core yet,
  //  see https://hydrolix.atlassian.net/browse/HDX-3779
  private val turbineCmdArgs = List(
    "hdx_reader",
    "--config", info.turbineIniPath,
    "--output_format", "json",
    "--fs_root", "/db/hdx",
    "--hdx_partition", scan.path,
    "--output_path", "-",
    "--schema", schemaStr
  ) ++ exprArgs

  private val hdxReaderProcessBuilder = Process(info.turbineCmdPath, turbineCmdArgs)

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
