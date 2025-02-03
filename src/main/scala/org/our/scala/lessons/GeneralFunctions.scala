package org.our.scala.lessons

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object GeneralFunctions {
  def import_csv_with_schema(spark: SparkSession, inferschema: StructType, path: String): DataFrame = {
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferschema", true)
      .schema(inferschema)
      .load(path)

    //Devuelvo Dataframe
    df
  }

  def import_csv_without_schema(spark: SparkSession, path: String): DataFrame = {
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      //.option("inferschema", true)
      .option("delimiter", ",")
      .load(path)

    //Devuelvo Dataframe
    df
  }
}
