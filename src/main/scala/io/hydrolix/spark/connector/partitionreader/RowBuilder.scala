package io.hydrolix.spark.connector.partitionreader

import com.fasterxml.jackson.databind.JsonNode

import io.hydrolix.connectors.types.{ArrayType, MapType, StructType, ValueType}

abstract class RowBuilder[R, A, M] {
  val `type`: StructType
  type AB <: ArrayBuilder
  type MB <: MapBuilder

  trait ArrayBuilder {
    val `type`: ArrayType
    def setNull(pos: Int)
    def set(pos: Int, value: Any)
    def build: A
  }

  trait MapBuilder {
    val `type`: MapType
    def putNull(key: Any)
    def put(key: Any, value: Any)
    def build: M
  }

  def newArrayBuilder(`type`: ArrayType): AB
  def newMapBuilder(`type`: MapType): MB
  def setNull(name: String)
  def setField(name: String, value: Any)
  def convertJsonValue(`type`: ValueType, jvalue: JsonNode): Any
  def build: R
}
