package main.scala.com.prannoy.paytmchallenge

import org.apache.log4j.LogManager
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object Main {

  @transient private lazy val logger = LogManager.getLogger(this.getClass)

  val maxSessionTimeInSeconds: Long = 15 * 60

  case class Result (f_id : String,f_timestamp  : Long, f_sid: Int)

  def main(args: Array[String]): Unit = {

    // creating a new spark session
    val spark=SparkSession.builder().master("local[*]").appName("Prannoy Tank : Paytm Web Log Challenge").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    //reading the file as csv from the data folder within the folder(file:///) , setting the schema , delimiter as space
    val df = spark
      .read
      .option("delimiter"," ")
      .schema(getSchema())
      .csv(s"file:///${System.getProperty("user.dir")}/data/2015_07_22_mktplace_shop_web_log_sample.log.gz")


    // Getting the sessionized dataframe
    val sessionizedDf = timeDifferenceFrameWise(df,spark)

    sessionizedDf.persist();

    //////////////////////////////////////////
    /// Average Session time per user
    //////////////////////////////////////////
    val firstLastSessionPerUserDf = sessionizedDf.groupBy($"client:port").agg(first("timestamp_epoch").as("first"), last("timestamp_epoch").as("last"),countDistinct("sess_id").as("total_unique_sessions"))

    //firstLastSessionPerUserDf.show(50,false)

    val averageSessionTimePerUserDf = firstLastSessionPerUserDf.withColumn("averageSessionTime", (col("last") - col("first"))/col("total_unique_sessions"))

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
    val firstLastSessionCountDf = sessionizedDf.groupBy($"client:port",$"sess_id").agg(first("timestamp_epoch").as("first"), last("timestamp_epoch").as("last"),count("sess_id").as("total_sessions"))

    //average and total time per user per session
    val totalTimePerSessionDf = firstLastSessionCountDf
      .withColumn("totalTimePerSession",col("last") - col("first"))
      //.withColumn("averageSessionTime", (col("last") - col("first"))/col("cnt"))

    //firstLastSessionCountDf.show(50,false)

    println("Most engaged user")
    // this will give most engaged user and the average session time
    totalTimePerSessionDf.orderBy($"totalTimePerSession".desc).show(50,false)


     sys.exit()

    val grouped = df.groupBy($"client:port" , window($"timestamp" , "2 minutes")).count
    grouped.select("client:port","window","count").orderBy($"client:port").show(50,false)
  }

  def plainScalaLogicMethod(): Unit ={

    val windowList = Seq(0, 3, 22, 24, 132, 290, 300, 495, 497, 517, 533, 693, 716, 730, 761, 807, 809, 888)


    val maxSessionTime = 20
    var entryIndex = 0
    var nextIndex = 1
    var sessionIndex = 0
    var sessionId = 1

    breakable
    {
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
          entryIndex += 1
          nextIndex += 1
        }
        else {
          println(s"1,${windowList(entryIndex)},${sessionId}")
          entryIndex += 1
          nextIndex += 1
        }
      }
    }
  }

  /**
   * closeset result
   * @param df
   * @param spark
   */
  def closesetResultSession(df: DataFrame,spark:SparkSession) = {
    import spark.implicits._

    // defining window partitions
    val login_window = Window.partitionBy("client:port").orderBy("timestamp")
    val session_window = Window.partitionBy("client:port", "session")

    //val session_window = Window.partitionBy("session").orderBy("timestamp")

    val session_df = df
      .withColumn("uid",concat(lit("uid"),lit("_"),dense_rank.over(Window.orderBy("client:port"))).cast("string"))
      .withColumn("session",concat($"uid",lit("_s_"),
      sum(
        (coalesce($"timestamp".cast("long") - lag($"timestamp".cast("long"), 1)
          .over(login_window), lit(0)) >= maxSessionTimeInSeconds)
          .cast("long")
      ).over(login_window).cast("string"))
    )


    //val result = session_df
    //.withColumn("became_active", unix_timestamp(min($"timestamp")).over(session_window))
    //.drop("session")
    //result.select("client:port","timestamp","user_agent","became_active","session").show(100,false)


    session_df.select("client:port","uid","timestamp","session").orderBy("client:port","timestamp").show(100,false)

  }



  /**
   * Time Difference Frame Wise
   * @param df
   * @param spark
   */
  def timeDifferenceFrameWise(df: DataFrame,spark:SparkSession): DataFrame ={
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
                break //todo: break is not supported
              }

              // if the element which the array index is at - the timestamp at session index is > max session time is true
              if ((clickList(nextIndex) - clickList(sessionIndex)) > maxSessionTime) {
                //insert the element at where the array is point to
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                //increment session id
                sessionId += 1
                // set the session index at the element where the array index was
                sessionIndex = nextIndex
                //increement next index and entry index
                entryIndex += 1
                nextIndex += 1
              }
              else {
                //insert the element the array is pointing to
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                //increment entry index and next index
                entryIndex += 1
                nextIndex += 1
              }
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
        explode(myFuncWithArg(maxSessionTimeInSeconds)(
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
