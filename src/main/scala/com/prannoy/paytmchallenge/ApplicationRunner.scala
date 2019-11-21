package com.prannoy.paytmchallenge

import com.prannoy.paytmchallenge.config.AppConfig
import com.prannoy.paytmchallenge.entity.AppEntity
import com.prannoy.paytmchallenge.services.TaskRunner.EtlTaskRunner
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, collect_list, concat, countDistinct, dense_rank, explode, first, lag, last, lit, row_number, sum, udf, window}

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

object ApplicationRunner {

  def apply(args: Array[String]) {
    AppConfig.parseApplicationConfig(args)
    runApp()
  }

  /**
   * Method which will execute different run-type
   */
  private def runApp() {

    val appConfig = AppConfig.getAppEntityObj

    for (task <- appConfig.getRunSequence.asScala) {
      task.toLowerCase() match {
        case s if s.contains("etl") =>{
          println("---------------------- ETL JOB START ----------------------------------")
          EtlTaskRunner.run()
          println("---------------------- ETL JOB END ------------------------------------")
        }
        case s if s.contains("streaming") => println("dummy spark streaming")
        case s if s.contains("ml") => println("dummy spark machine learning")
        case _ => println("Task type not supported yet")
      }
    }


  }

  def plainScalaLogicMethod(): Unit = {

    val windowList = Seq(0, 3, 22, 24, 132, 290, 300, 495, 497, 517, 533, 693, 716, 730, 761, 807, 809, 888)


    val maxSessionTime = 20
    var entryIndex = 0
    var nextIndex = 1
    var sessionIndex = 0
    var sessionId = 1

    breakable {
      for (i <- 0 until windowList.length) {

        if (entryIndex == windowList.size - 1) {
          if ((windowList(sessionIndex) - windowList(entryIndex)) > maxSessionTime) {
            println(s"1,${windowList(entryIndex)},${sessionId + 1}")
          }
          else {
            println(s"1,${windowList(entryIndex)},${sessionId}")
          }
          break
        }

        if ((windowList(nextIndex) - windowList(sessionIndex)) > maxSessionTime) {
          println(s"1,${windowList(entryIndex)},${sessionId}")
          sessionId += 1
          sessionIndex = nextIndex
        }
        else {
          println(s"1,${windowList(entryIndex)},${sessionId}")
        }
        entryIndex += 1
        nextIndex += 1
      }
    }
  }

//  /**
//   * closeset result
//   *
//   * @param df
//   * @param spark
//   */
//  def closesetResultSession(df: DataFrame, spark: SparkSession) = {
//    import spark.implicits._
//
//    // defining window partitions
//    val login_window = Window.partitionBy("client:port").orderBy("timestamp")
//    val session_window = Window.partitionBy("client:port", "session")
//
//    //val session_window = Window.partitionBy("session").orderBy("timestamp")
//
//    val session_df = df
//      .withColumn("uid", concat(lit("uid"), lit("_"), dense_rank.over(Window.orderBy("client:port"))).cast("string"))
//      .withColumn("session", concat($"uid", lit("_s_"),
//        sum(
//          (coalesce($"timestamp".cast("long") - lag($"timestamp".cast("long"), 1)
//            .over(login_window), lit(0)) >= appEntityObj.getMaxSessionTime)
//            .cast("long")
//        ).over(login_window).cast("string"))
//      )
//
//
//    //val result = session_df
//    //.withColumn("became_active", unix_timestamp(min($"timestamp")).over(session_window))
//    //.drop("session")
//    //result.select("client:port","timestamp","user_agent","became_active","session").show(100,false)
//
//
//    session_df.select("client:port", "uid", "timestamp", "session").orderBy("client:port", "timestamp").show(100, false)
//
//  }

}

