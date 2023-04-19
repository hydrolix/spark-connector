package io.hydrolix.spark

import model.HdxConnectionInfo

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.ArrayBlockingQueue
import scala.jdk.CollectionConverters._
import scala.sys.error
import scala.sys.process.{Process, ProcessLogger}

// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
// TODO make a ColumnarBatch version too
class HdxPartitionReader(info: HdxConnectionInfo,
                    partition: HdxPartition)
  extends PartitionReader[InternalRow]
    with Logging
{
  log.info(s"Requested partition ${partition.path}")

  private val processBuilder = Process(
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

  private val doneSignal = "DONE"
  private val q = new ArrayBlockingQueue[String](1024)
  @volatile private var exitCode: Option[Int] = None

  private val proc = processBuilder.run(ProcessLogger(
    { q.put }, // TODO this relies on the stdout being split into strings; that won't be the case once we get gzip working!
    { log.warn("hdx_reader stderr: {}", _) }
  ))
  new Thread(() => {
    exitCode = Some(proc.exitValue()) // Blocks until exit
    q.put(doneSignal)
  }).start()

  // The contract says get() should always return the same record if called multiple times per next()
  private var rec: InternalRow = _
  override def next(): Boolean = {
    // It's OK to block here, there will always be at least doneSignal
    val s = q.take() // TODO maybe set a time limit instead of potentially blocking infinitely
    if (s eq doneSignal) {
      false
    } else {
      rec = row(s)
      true
    }
  }

  override def get(): InternalRow = rec

  private def row(s: String) = {
    val obj = JSON.objectMapper.readValue[ObjectNode](s)

    val values = partition.schema.map { col =>
      val node = obj.get(col.name) // TODO can we be sure the names match?
      node2Any(node, col.name, col.dataType)
    }

    InternalRow.fromSeq(values)
  }

  private def node2Any(node: JsonNode, name: String, dt: DataType): Any = {
    node match {
      case n if n.isNull => null
      case s: TextNode => UTF8String.fromString(s.textValue())
      case l: LongNode => l.longValue()
      case i: IntNode => i.intValue()
      case d: DoubleNode => d.doubleValue()
      case b: BooleanNode => b.booleanValue()
      case bd: DecimalNode => bd.decimalValue()
      case a: ArrayNode =>
        dt match {
          case ArrayType(elementType, _) =>
            val values = a.asScala.map(node2Any(_, name, elementType)).toList
            new GenericArrayData(values)

          case other => error(s"TODO JSON array field $name needs conversion from $other to $dt")
        }
      case obj: ObjectNode =>
        dt match {
          case MapType(keyType, valueType, _) =>
            if (keyType != DataTypes.StringType) error(s"TODO JSON map field $name keys are $keyType, not strings")
            val keys = obj.fieldNames().asScala.map(UTF8String.fromString).toArray
            val values = obj.fields().asScala.map(entry => node2Any(entry.getValue, name, valueType)).toArray
            new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))

          case other => error(s"TODO JSON map field $name needs conversion from $other to $dt")
        }
    }
  }

  override def close(): Unit = {
    if (proc.isAlive()) {
      log.info("Killing child process")
      proc.destroy()
    }
    if (proc.exitValue() != 0) {
      log.warn(s"Child process exited with value ${proc.exitValue()}")
    }
  }
}
