package io.hydrolix.spark.connector.partitionreader

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.hydrolix.connectors.types.{ArrayType, MapType, StructType, ValueType}

trait RowAdapter[R, A, M] {
  type RB <: RowBuilder
  type AB <: ArrayBuilder
  type MB <: MapBuilder

  trait RowBuilder {
    val `type`: StructType

    def setNull(name: String)

    def setField(name: String, value: Any)

    def build: R
  }

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

  def row(`type`: StructType, obj: ObjectNode): R

  def newRowBuilder(`type`: StructType): RB
  def newArrayBuilder(`type`: ArrayType): AB
  def newMapBuilder(`type`: MapType): MB
  def convertJsonValue(`type`: ValueType, jvalue: JsonNode): Any
}
