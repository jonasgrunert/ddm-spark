package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val dataframes=inputs.map(path=>spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
    )

    //TODO 
  }
}
