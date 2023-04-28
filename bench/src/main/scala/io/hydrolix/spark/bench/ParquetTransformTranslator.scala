package io.hydrolix.spark.bench

import io.hydrolix.spark.model.{HdxColumnDatatype, HdxOutputColumn, JSON}

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

import java.net.URI
import scala.collection.JavaConverters._

/**
 * args:
 *  1. gs:// URL
 *  1. path to service account key file
 */
object ParquetTransformTranslator extends App {
  private val googleUrlR = """gs://(.*?)/(.*?)$""".r
  private val log = LoggerFactory.getLogger(getClass)

  args(0) match {
    case googleUrlR(_, _) =>
      val conf = new Configuration()

      // Note: these seem to be required even for public buckets; otherwise it just times out without doing anything
      conf.set("google.cloud.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
      conf.set("google.cloud.auth.service.account.json.keyfile", args(1))

      val fs = new GoogleHadoopFileSystem()
      val uri = new URI(args(0))
      fs.initialize(uri, conf)
      val it = fs.listFiles(new Path(uri), true)
      while (it.hasNext) {
        val file = it.next()
        val path = file.getPath

        if (path.getName.endsWith(".parquet")) {
          val f = HadoopInputFile.fromPath(path, conf)
          val reader = new ParquetFileReader(f, ParquetReadOptions.builder().build())
          val schema = reader.getFileMetaData.getSchema

          println(path)
          println(schema)
          println()

          val outCols = for ((fieldType, i) <- schema.getFields.asScala.toList.zipWithIndex) yield {
            val name = schema.getFieldName(i)
            HdxOutputColumn(name, parquetToHdx(name, fieldType))
          }

          val timestamps = outCols.filter(_.datatype.`type` == "datetime64")
          val outCols2 = if (timestamps.isEmpty) {
            log.warn(s"No timestamp field found; making a fake one")
            outCols ++ Some(HdxOutputColumn("_fake_timestamp", HdxColumnDatatype.apply("datetime64", true, true, resolution = Some("ms"), format = Some("2006-01-02T15:04:05.999"))))
          } else if (timestamps.size == 1) {
            outCols
          } else {
            val it = timestamps.head
            log.warn(s"Multiple timestamp fields found; arbitrarily setting the first (${it.name}) as primary")
            outCols.map { col =>
              if (col.name == it.name) {
                col.copy(datatype = col.datatype.copy(primary = true))
              } else {
                col
              }
            }
          }

          println(JSON.objectMapper.writeValueAsString(outCols2))
          println()
        } else {
          println(s"Skipping $path")
        }
      }
  }


  private def parquetToHdx(name: String, typ: Type): HdxColumnDatatype = {
    typ.getLogicalTypeAnnotation match {
      case null =>
        typ match {
          case p: PrimitiveType =>
            p.getPrimitiveTypeName match {
              case PrimitiveTypeName.FLOAT => HdxColumnDatatype("double", false, false)
              case PrimitiveTypeName.DOUBLE => HdxColumnDatatype("double", false, false)
              case PrimitiveTypeName.INT32 => HdxColumnDatatype("int32", false, false)
              case PrimitiveTypeName.INT64 => HdxColumnDatatype("int64", false, false)
              case PrimitiveTypeName.BOOLEAN => HdxColumnDatatype("int8", false, false)
            }
        }

      case ilt: IntLogicalTypeAnnotation =>
        val suf = ilt.getBitWidth match {
          case 8 => "int8"
          case 16 | 32 => "int32"
          case 64 => "int64"
        }

        val pre = if (ilt.isSigned) "u" else ""

        HdxColumnDatatype(pre + suf, false, false)

      case _: StringLogicalTypeAnnotation =>
        HdxColumnDatatype("string", false, false)

      case _: DateLogicalTypeAnnotation =>
        HdxColumnDatatype("datetime", false, false, format = Some("2006-01-02"))

      case d: DecimalLogicalTypeAnnotation =>
        if (d.getPrecision > 18) {
          log.warn(s"Decimal field $name has precision ${d.getPrecision}, may not fit in a `double`")
        }
        HdxColumnDatatype("double", false, false)

      case ts: TimestampLogicalTypeAnnotation if ts.isAdjustedToUTC && ts.getUnit == LogicalTypeAnnotation.TimeUnit.MILLIS =>
        // TODO Parquet doesn't support less than millis resolution, figure out where to put the exceptions
        HdxColumnDatatype("datetime64", false, false, resolution = Some("ms"), format = Some("2006-01-02T15:04:05.999"))

      case _: MapLogicalTypeAnnotation =>
        val keyType = parquetToHdx(name, typ.asGroupType().getType(0))
        val valueType = parquetToHdx(name, typ.asGroupType().getType(1))

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](keyType))
        arr.add(JSON.objectMapper.convertValue[JsonNode](valueType))
        HdxColumnDatatype(s"map<${keyType.`type`},${valueType.`type`}>", false, false, elements = Some(arr))

      case _: ListLogicalTypeAnnotation =>
        val elementType = parquetToHdx(name, typ.asGroupType().getType(0))

        val arr = JSON.objectMapper.getNodeFactory.arrayNode()
        arr.add(JSON.objectMapper.convertValue[JsonNode](elementType))
        HdxColumnDatatype(s"array<${elementType.`type`}>", false, false, elements = Some(arr))
    }
  }

}
