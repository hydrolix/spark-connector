package io.hydrolix.spark

import model.HdxConnectionInfo

import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.cloud.storage.StorageOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

case class HdxPartition(db: String,
                     table: String,
                      path: String,
                    pkName: String,
                      cols: Set[String])
  extends InputPartition

class HdxPartitionReaderFactory(info: HdxConnectionInfo) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new HdxPartitionReader(info, partition.asInstanceOf[HdxPartition])
  }
}

class HdxPartitionReader(info: HdxConnectionInfo,
                    partition: HdxPartition)
  extends PartitionReader[InternalRow]
     with Logging
{
  private val xx = info.cloud match {
    case "gcp" =>
      val partitionDir = s"db/hdx/${partition.path}"
      val storageOpts = StorageOptions.newBuilder()
        .setCredentials(GoogleCredentials.create(AccessToken.newBuilder().setTokenValue(info.cloudCred1).build()))
        .build()
      val storage = storageOpts.getService
      val manifest = storage.get(info.bucketPrefix, s"$partitionDir/manifest.hdx")
      val index = storage.get(info.bucketPrefix, s"$partitionDir/index.hdx")
      val data = storage.get(info.bucketPrefix, s"$partitionDir/data.hdx")

      println(s"Manifest file: $manifest")
      println(s"Index file: $index")
      println(s"Data file: $data")
  }

  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}
