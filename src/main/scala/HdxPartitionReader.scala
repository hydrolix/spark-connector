package io.hydrolix.spark

import model.HdxConnectionInfo

import com.fasterxml.jackson.databind.node.{DoubleNode, LongNode, NullNode, ObjectNode, TextNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.util.concurrent.ArrayBlockingQueue
import scala.sys.process.{Process, ProcessLogger}

// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
class HdxPartitionReader(info: HdxConnectionInfo,
                         partition: HdxPartition)
  extends PartitionReader[InternalRow]
    with Logging {
  log.info(s"Requested partition $partition")

  val processBuilder = Process(
    info.turbineCmdPath.toString,
    List(
      "hdx_reader",
      "--config", info.turbineIniPath.toString,
      "--output_format", "json",
      "--fs_root", "/db/hdx",
      "--hdx_partition", partition.path,
      "--output_path", "-"
    )
  )

  val q = new ArrayBlockingQueue[String](1024)

  val proc = processBuilder.run(ProcessLogger(
    { line =>
      log.info("hdx_reader stdout: {}", line)
      q.put(line)
    }, // TODO this relies on the stdout being split into strings; that won't be the case once we get gzip working!
    { line =>
      log.warn("hdx_reader stderr: {}", line)
    }
  ))

  override def next(): Boolean = {
    !q.isEmpty && proc.isAlive()
  }

  override def get(): InternalRow = {
    val s = q.take()
    log.info(s)
    val obj = JSON.objectMapper.readValue[ObjectNode](s)

    val values = partition.cols.map { col =>
      obj.get(col) match {
        case n: NullNode => null
        case s: TextNode => s.textValue()
        case l: LongNode => l.longValue()
        case d: DoubleNode => d.doubleValue()
      }
    }
    InternalRow(values)
  }

  override def close(): Unit = ???
}
