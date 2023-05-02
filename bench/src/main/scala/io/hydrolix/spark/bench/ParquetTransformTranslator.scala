package io.hydrolix.spark.bench

import io.hydrolix.spark.model._

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType, Type}
import org.slf4j.LoggerFactory

import java.io.{FileOutputStream, OutputStreamWriter}
import java.net.URI
import java.nio.file.{Path => jPath}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * TODO:
 *  - other clouds
 *  - figure out some of the ambiguous/wrong stuff
 *
 * args:
 *  1. `gs://` URL, e.g. `gs://beam-tpcds/datasets/parquet/nonpartitioned/1GB/`
 *  1. Local filesystem path to service account key file
 *  1. Local filesystem path to output transform JSON files. Must be a writable directory; will be created if possible.
 */
object ParquetTransformTranslator extends App {
  private val googleUrlR = """gs://(.*?)/(.*?)$""".r
  private val log = LoggerFactory.getLogger(getClass)

  private val outPath = jPath.of(args(2)).toFile
  outPath.mkdirs()
  if (!outPath.canWrite || !outPath.isDirectory) {
    sys.error(s"${outPath.getAbsolutePath} doesn't exist and/or isn't writable")
  }

  args(0) match {
    case googleUrlR(_, _) =>
      val conf = new Configuration()

      // Note: these seem to be required even for public buckets; otherwise it just times out without doing anything
      conf.set("google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
      conf.set("google.cloud.auth.service.account.json.keyfile", args(1))

      val tableTransforms = mutable.Map[String, HdxTransform]()

      val fs = new GoogleHadoopFileSystem()
      val rootUri = new URI(args(0))
      val rootUriS = rootUri.toString.reverse.dropWhile(_ == '/').reverse
      fs.initialize(rootUri, conf)

      val it = fs.listFiles(new Path(rootUri), true)
      while (it.hasNext) {
        val file = it.next()
        val path = file.getPath

        if (path.getName.endsWith(".parquet")) {
          // Try to guess the table name by finding a path component between the prefix and filename

          val pathUriS = path.toUri.toString
          val maybeTableName = if (pathUriS.startsWith(rootUriS)) {
            val rel = pathUriS.substring(rootUriS.length + 1)
            rel.dropRight(path.getName.length + 1)
          } else {
            path.getName
          }

          val f = HadoopInputFile.fromPath(path, conf)
          val reader = new ParquetFileReader(f, ParquetReadOptions.builder().build())
          val schema = reader.getFileMetaData.getSchema
          log.info(s"Schema for $path: $schema")

          val outCols = for ((fieldType, i) <- schema.getFields.asScala.toList.zipWithIndex) yield {
            val name = schema.getFieldName(i)
            HdxOutputColumn(name, parquetToHdx(name, fieldType))
          }

          val timestamps = outCols.filter(_.datatype.`type` == HdxValueType.DateTime64)
          val outCols2 = if (timestamps.isEmpty) {
            log.info(s"No timestamp field found in $maybeTableName; making a fake one")
            outCols ++ Some(HdxOutputColumn("_fake_timestamp", HdxColumnDatatype.apply(HdxValueType.DateTime64, true, true, resolution = Some("ms"), format = Some("2006-01-02T15:04:05.999"))))
          } else if (timestamps.size == 1) {
            outCols
          } else {
            val it = timestamps.head
            log.warn(s"Multiple timestamp fields found in $maybeTableName; arbitrarily setting the first (${it.name}) as primary")
            outCols.map { col =>
              if (col.name == it.name) {
                col.copy(datatype = col.datatype.copy(primary = true))
              } else {
                col
              }
            }
          }

          val transform = HdxTransform(
            maybeTableName,
            None,
            HdxTransformType.json,
            maybeTableName,
            HdxTransformSettings(
              true,
              HdxTransformCompression.none,
              None,
              Nil,
              JsonFormatDetails(JsonFlattening(false, None, None, None)),
              outCols2
            )
          )

          tableTransforms.get(maybeTableName) match {
            case Some(existing) =>
              if (existing == transform) {
                log.info(s"We already had a transform for $maybeTableName but it's identical, no biggie")
              } else {
                log.warn(s"Table $maybeTableName was seen twice, and had different transforms each time!")
              }
            case None =>
              tableTransforms.put(maybeTableName, transform)

              val s = JSON.objectMapper.convertValue[JsonNode](transform).toPrettyString
              val file = new java.io.File(outPath, s"$maybeTableName.json")
              val w = new OutputStreamWriter(new FileOutputStream(file))
              w.write(s)
              w.close()
              log.info(s"Wrote ${file.getAbsolutePath}")
          }
        } else {
          log.info(s"Skipping $path")
        }
      }
  }


  private def parquetToHdx(name: String, typ: Type): HdxColumnDatatype = {
    typ.getLogicalTypeAnnotation match {
      case null =>
        typ match {
          case p: PrimitiveType =>
            p.getPrimitiveTypeName match {
              case PrimitiveTypeName.FLOAT => HdxColumnDatatype(HdxValueType.Double, false, false)
              case PrimitiveTypeName.DOUBLE => HdxColumnDatatype(HdxValueType.Double, false, false)
              case PrimitiveTypeName.INT32 => HdxColumnDatatype(HdxValueType.Int32, false, false)
              case PrimitiveTypeName.INT64 => HdxColumnDatatype(HdxValueType.Int64, false, false)
              case PrimitiveTypeName.BOOLEAN => HdxColumnDatatype(HdxValueType.Int8, false, false)
              case _ => sys.error(s"Can't map Parquet column $name: $typ")
            }
        }

      case ilt: IntLogicalTypeAnnotation =>
        val suf = ilt.getBitWidth match {
          case 8 => "int8"
          case 16 | 32 => "int32"
          case 64 => "int64"
        }

        val pre = if (ilt.isSigned) "u" else ""

        HdxColumnDatatype(HdxValueType.forName(pre + suf), false, false)

      case _: StringLogicalTypeAnnotation =>
        HdxColumnDatatype(HdxValueType.String, false, false)

      case _: DateLogicalTypeAnnotation =>
        HdxColumnDatatype(HdxValueType.DateTime, false, false, format = Some("2006-01-02"))

      case d: DecimalLogicalTypeAnnotation =>
        if (d.getPrecision > 18) {
          log.warn(s"Decimal field $name has precision ${d.getPrecision}, may not fit in a `double`")
        }
        HdxColumnDatatype(HdxValueType.Double, false, false)

      case ts: TimestampLogicalTypeAnnotation if ts.isAdjustedToUTC && ts.getUnit == LogicalTypeAnnotation.TimeUnit.MILLIS =>
        // TODO Parquet doesn't support less than millis resolution, figure out where to put the exceptions
        HdxColumnDatatype(HdxValueType.DateTime64, false, false, resolution = Some("ms"), format = Some("2006-01-02T15:04:05.999"))

      case _: MapLogicalTypeAnnotation =>
        val keyType = parquetToHdx(name, typ.asGroupType().getType(0))
        val valueType = parquetToHdx(name, typ.asGroupType().getType(1))

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](keyType))
        arr.add(JSON.objectMapper.convertValue[JsonNode](valueType))
        HdxColumnDatatype(HdxValueType.Map, false, false, elements = Some(arr))

      case _: ListLogicalTypeAnnotation =>
        val elementType = parquetToHdx(name, typ.asGroupType().getType(0))

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](elementType))
        HdxColumnDatatype(HdxValueType.Array, false, false, elements = Some(arr))
    }
  }

}
