package io.hydrolix.spark.bench.tpc

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.HttpStorageOptions
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem

import java.io.FileInputStream
import java.net.URI
import scala.collection.JavaConverters._

object TPCTransformGenerator extends App {
  private val googleUrlR = """gs:\/\/(.*?)\/(.*?)$""".r

  args(0) match {
    case googleUrlR(bucket, prefix) =>
      val creds = ServiceAccountCredentials.fromStream(new FileInputStream(args(1)))

      val storageOptions = HttpStorageOptions.newBuilder()
        .setCredentials(creds)
        .build()

      val storage = storageOptions.getService

      val fs = new GoogleHadoopFileSystem()

      val blobs = storage.list(bucket, BlobListOption.prefix(prefix))
      for (blob <- blobs.iterateAll().asScala) {
        val info = blob.asBlobInfo()
        val hpath = fs.getHadoopPath(new URI(info.getSelfLink))

        println(s"Blob info: $info")
        println(s"Hadoop path: $hpath")
      }


  }
}
