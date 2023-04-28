package io.hydrolix.spark.connector

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

object JSON {
  val objectMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .addModule(new JavaTimeModule())
    .build() :: ClassTagExtensions
}
