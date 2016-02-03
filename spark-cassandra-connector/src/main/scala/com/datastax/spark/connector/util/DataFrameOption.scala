package com.datastax.spark.connector.util

trait DataFrameOption {
  val optionName: String

  def apply(value: Any): Map[String, String] = {
    Map(optionName -> value.toString)
  }
}
