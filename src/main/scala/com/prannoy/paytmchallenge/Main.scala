package main.scala.com.prannoy.paytmchallenge

import com.prannoy.paytmchallenge.ApplicationRunner
import com.prannoy.paytmchallenge.entity.AppEntity
import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object Main {

  @transient private lazy val logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    println("----------------- START: APPLICATION -----------------------------")

    ApplicationRunner.apply(args);

    println("----------------- END: APPLICATION -------------------------------")
  }
}
