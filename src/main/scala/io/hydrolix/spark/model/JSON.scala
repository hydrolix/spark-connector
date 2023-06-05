package io.hydrolix.spark.model

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

object JSON {
  val objectMapper: JsonMapper with ClassTagExtensions = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS) // For HdxValueType mostly, since `double` and `boolean` are taken
    .build() :: ClassTagExtensions
}
