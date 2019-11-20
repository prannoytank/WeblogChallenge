package com.prannoy.paytmchallenge.services.TaskRunner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat, countDistinct, dense_rank, explode, first, last, lit, row_number, udf}
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object EtlTaskRunner extends TaskRunnerTrait {

  case class Result (f_id : String,f_timestamp  : Long, f_sid: Int)

  override def run(): Unit = {

    // creating a new spark session
    val spark=SparkSession.builder().master("local[*]").appName("Prannoy Tank : Paytm Web Log Challenge").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    //reading the file as csv from the data folder within the folder(file:///) , setting the schema , delimiter as space
    val df = spark
      .read
      .option("delimiter"," ")
      .schema(getSchema())
      .csv(appEntityObj.getFilePath)

    // Getting the sessionized dataframe
    val sessionizedDf = sessionizeWebLogs(df,spark)

    sessionizedDf.persist();

    //////////////////////////////////////////
    /// Average Session time per user
    //////////////////////////////////////////
    val firstLastSessionPerUserDf = sessionizedDf.groupBy($"client:port").agg(first("timestamp_epoch").as("first"), last("timestamp_epoch").as("last"),countDistinct("sess_id").as("total_unique_sessions"))

    //firstLastSessionPerUserDf.show(50,false)

    val averageSessionTimePerUserDf = firstLastSessionPerUserDf.withColumn("AverageSessionTimeInSeconds", (col("last") - col("first"))/col("total_unique_sessions"))

    println("Average Session Time Per User")
    averageSessionTimePerUserDf.show(50,false)


    //////////////////////////////////////////
    //// unique url visit count per user per session
    //////////////////////////////////////////
    val uniqueUrlVisitPerSesion=sessionizedDf.groupBy($"client:port",$"sess_id").agg(countDistinct($"request").as("count"))

    println("Unique Url Visit Per User Per Session")
    uniqueUrlVisitPerSesion.show(50,false)


    //////////////////////////////////////////
    //// Most Engaged user with max time per user per session
    //////////////////////////////////////////
    val firstLastSessionCountDf = sessionizedDf.groupBy($"client:port",$"sess_id").agg(first("timestamp_epoch").as("first"), last("timestamp_epoch").as("last"))

    //average and total time per user per session
    val totalTimePerSessionDf = firstLastSessionCountDf
      .withColumn("MaxSessionTimeInSeconds",col("last") - col("first"))
    //.withColumn("averageSessionTime", (col("last") - col("first"))/col("cnt"))

    println("Most engaged user")
    // this will give most engaged user and the average session time
    totalTimePerSessionDf.orderBy($"MaxSessionTimeInSeconds".desc).show(50,false)


  }

  /**
   * sessionizeWebLogs
   * @param df
   * @param spark
   */
  def sessionizeWebLogs(df: DataFrame,spark:SparkSession): DataFrame ={
    import spark.implicits._


    /**
     * Udf currying wrapper Function
     * @param maxSessionTime : max Session time.
     * @return
     */
    def myFuncWithArg(maxSessionTime: Long) = {

      /**
       * Main Udf Function:
       * Takes unique user id which is created from the client:port column
       * and list of all the timestamp for that user/client:port in long format
       * returns the result (uid,timestamp,sessionid)
       */
      udf((uid: String, clickList: Seq[Long]) => {

        var sessionsIdList: ListBuffer[Result] = ListBuffer()

        // if the size of timestamp array is 1 , return the session id as 1
        if (clickList.length <= 1) {
          ListBuffer(Result(uid, clickList(0), 1))
        }

        else {

          //val maxSessionTime = tmo
          var entryIndex = 0 //to track , what to insert in the new array
          var nextIndex = 1 //next index
          var sessionIndex = 0 // session index to track from where the new session starts
          var sessionId = 1 // session id which increments as and when new session starts based on the max session time

          breakable
          {
            for (i <- 0 until clickList.length) {
              //println(i)

              // checking the last element has been reached from the array
              if (entryIndex == clickList.size - 1) {
                if ((clickList(sessionIndex) - clickList(entryIndex)) > maxSessionTime) {
                  sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId + 1))
                }
                else {
                  sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                }
                break
              }

              // if the element which the array index is at - the timestamp at session index is > max session time is true
              if ((clickList(nextIndex) - clickList(sessionIndex)) > maxSessionTime) {

                //insert the element at where the array is point to
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                //increment session id
                sessionId += 1
                // set the session index at the element where the array index was
                sessionIndex = nextIndex
              }
              else {
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
              }
              //increment entry index and next index
              entryIndex += 1
              nextIndex += 1

            }
          }
          (sessionsIdList) //else return statement
        } // end of else
      }) // end of udf
    } // end of function

    // window partition by client:port and order by timestamp in assending order
    val windowSpec = Window.partitionBy("client:port").orderBy("timestamp")

    val r1=df
      .withColumn("uid",concat(lit("u"),lit("_"),dense_rank.over(Window.orderBy("client:port"))).cast("string")) // create unqiue user id
      .withColumn("row_number", row_number() over windowSpec) // assign unique row numbers to each group by frame
      .withColumn("timestamp_epoch",$"timestamp".cast("long")) //casting timestamp to epoch/long format
    //.withColumn("time_diff",$"timestamp".cast("long") - lag($"timestamp", 1).over(windowSpec).cast("long"))
    //.withColumn("time_diff_in_a_frame",$"timestamp".cast("long") - first($"timestamp".cast("long"),true).over(windowSpec))

    val df2 = r1.
      groupBy("uid").agg(collect_list($"timestamp_epoch").as("timestamp_epoch")) // collecting the list of timestamp per user
      .withColumn("click_sess_id",
        explode(myFuncWithArg(appEntityObj.getMaxSessionTime)(
          //struct(r1.col("uid"),r1.col("timestamp_epoch"),r1.col("time_diff_in_a_frame")))
          $"uid", $"timestamp_epoch")) //calling the custom udf by passing user id and list of timestamp
      )
      .select($"uid", $"click_sess_id.f_timestamp".as("timestamp_epoch"), $"click_sess_id.f_sid".as("sess_id"))

    val df3 = df2.alias("a").join(r1.alias("b"),Seq("uid","timestamp_epoch")) // joining with original dataframe
    df3
  }

  /**
   * Schema for web log
   * Referred: https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
   * @return StructType
   */
  def  getSchema(): StructType ={

    val webLogDecimalType = DataTypes.createDecimalType(9, 6)

    val webLogSchema = StructType(Array(
      StructField("timestamp",    TimestampType, true),
      StructField("elb ",   StringType, true),
      StructField("client:port",  StringType, true),
      StructField("backend:port",   StringType, true),
      StructField("request_processing_time",  webLogDecimalType, true),
      StructField("backend_processing_time",  webLogDecimalType, true),
      StructField("response_processing_time",  webLogDecimalType, true),
      StructField("elb_status_code",    IntegerType, true),
      StructField("backend_status_code",   IntegerType, true),
      StructField("received_bytes",  LongType, true),
      StructField("sent_bytes",   LongType, true),
      StructField("request",StringType, true),
      StructField("user_agent",  StringType, true),
      StructField("ssl_cipher",  StringType, true),
      StructField("ssl_protocol",  StringType, true)

    ))

    webLogSchema
  }

}