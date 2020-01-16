package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.nio.file
import java.nio.file.{Path, Paths}

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val optionsPath: Symbol = 'path;
    val optionsCore: Symbol = 'cores;
    // CLI Parsing done like this: https://stackoverflow.com/a/3183991
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--path" :: value :: tail =>
          nextOption(map ++ Map(optionsPath -> value), tail)
        case "--cores" :: value :: tail =>
          nextOption(map ++ Map(optionsCore -> value.toInt), tail)
      }
    }
    // options now has the appropriate options and defaults
    val options = nextOption(Map(optionsPath -> "TPCH", optionsCore -> 4),arglist)

    // Getting all Filenames from the specified folder

    val currDir = Paths.get(System.getProperty("user.dir"), options.get(optionsPath).get.toString).toFile.listFiles();


    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[".concat(options.get(optionsCore).get.toString).concat("]")) // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()


    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    //TODO read all the others too
    val inputs = List("region", "nation"/*, "supplier", "customer", "part", "lineitem", "orders"*/)
      .map(name => s"data/TPCH/tpch_$name.csv")

    Sindy.discoverINDs(inputs, spark)
  }
}
