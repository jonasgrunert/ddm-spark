package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    // Reading the data
    val dataframes: List[DataFrame] = inputs.map(path=>spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
    )

    // ####################################################################### Figure 1

    // Step One: Creating tuples of (value, column_name) e.g. (AFRICA, R_NAME)
    val candidatesTuples: Unit = dataframes
      .map(df => {
        // Column Names as Sets so we can use Set operations later on
        val columns = df.columns.map(Set(_))
        // return is implicit
        df
          .rdd
          // The zip operator applies one column name repeatedly to one value in a row
          .flatMap(row => row.toSeq.map(_.toString) zip columns)
          // Step Two: For every same value collect all columnnames (value, columns_names)
          // e.g. (0, {N_NATIONKEY, N_REGIONKEY, R_REGIONKEY)
          // That groups them by key, so same values together
          // Takes in a iterable of multiple Sets) and combines them into (String, all unique values from all Sets)
          .reduceByKey(_++_)
          .toDS
      })
      // Step Three: We partition globally
      .reduce(_ union _)
      .rdd
      // Step Four: We build only attribute Sets
      .reduceByKey(_++_)
      .map(_._2)
      // DataStructure is like this now: Set(Set(attributes))
    // ####################################################################### Figure 2
      // Step Five: Building Inclusion Lists
      // for every Set of AttributeNames map over all columnNames and filter all columnNames for that one
      .flatMap(columnNames => columnNames
        .map(columnName =>
          (columnName, columnNames
            .filter(!columnName.equals(_))
          )
        )
      )
      // Step Six: partition by key
      // Step Seven: We aggregate by intersecting
      // We now only build intersections of the set as
      .reduceByKey(_ intersect _)
      // We sort out empty Sets
      .filter(_._2.nonEmpty)
      // We make a string out of them
      .map(ind => ind._1 + " < " + ind._2.mkString(", "))
      .collect
      // We sort them by key alphabetically
      .sorted
      .foreach(println)
  }
}
