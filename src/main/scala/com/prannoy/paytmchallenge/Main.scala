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

  val tmo1: Long = 5 * 60


  case class Result (
                      f_id : String,
                      f_timestamp  : Long,
                      f_sid    : Int
                    );

  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("Paytm Web Log Challenge").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val tmo2: Long = 60 * 60

    val df = spark
      .read
      .option("delimiter"," ")
      .schema(getSchema())
      .csv("file:////home/cloudera/projects/WeblogChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    //rowsLastWindow(df,spark)
    //queryBasedWindows(df,spark);
   timeDifferenceFrameWise(df,spark)

    //plainScalaLogicMethod()
    //closesetResultSession(df,spark)

    //workingSession(df,spark)

   sys.exit()

    val grouped = df.groupBy($"client:port" , window($"timestamp" , "2 minutes")).count
    grouped.select("client:port","window","count").orderBy($"client:port").show(50,false)


    val windowSpec = Window.partitionBy("client:port").orderBy("timestamp")
    val session_window = Window.partitionBy("session").orderBy("timestamp")
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



  def workingSession(df:DataFrame,spark:SparkSession) = {

    import spark.implicits._
    def clickSessList(tmo: Long) = udf{ (uid: String, clickList: Seq[Long], tsList: Seq[Long]) =>
      def sid(n: Long) = s"$uid-$n"

      val sessList = tsList.foldLeft( (List[String](), 0L, 0L) ){ case ((ls, j, k), i) =>
        if (i == 0) (sid(k + 1) :: ls, 0L, k + 1) else
        if (j + i < tmo) (sid(k) :: ls, j + i, k) else
          (sid(k + 1) :: ls, 0L, k + 1)
      }._1.reverse

      clickList zip sessList
    }

    val win1 = Window.partitionBy("client:port").orderBy("timestamp")

    val df1 = df.
      withColumn("ts_diff", unix_timestamp($"timestamp") - unix_timestamp(
        lag($"timestamp", 1).over(win1))
      )
      .withColumn("timestamp_epoch",$"timestamp".cast("long"))
      .withColumn("time_diff",$"timestamp".cast("long") - lag($"timestamp", 1).over(win1).cast("long"))
      .withColumn("time_diff_in_a_frame",$"timestamp".cast("long") - first($"timestamp".cast("long"),true).over(win1))
//      withColumn("ts_diff", when(row_number.over(win1) === 1 || $"ts_diff" >= tmo1, 0L).
//        otherwise($"ts_diff")
//      )

    df1.select("client:port","timestamp","ts_diff").orderBy("client:port","timestamp").show(30,false)

    val df2 = df1.
      groupBy("client:port").agg(
      collect_list($"timestamp_epoch").as("click_list"), collect_list($"time_diff_in_a_frame").as("ts_list")
    ).
      withColumn("click_sess_id",
        explode(clickSessList(tmo1)($"client:port", $"click_list", $"ts_list"))
      ).
      select($"client:port", $"click_sess_id._1".as("click_time"), $"click_sess_id._2".as("sess_id"))

    df2.orderBy($"client:port",$"click_time").show(30,false)
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
          .over(login_window), lit(0)) >= tmo1)
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
  def timeDifferenceFrameWise(df: DataFrame,spark:SparkSession): Unit ={
    import spark.implicits._


    def myFuncWithArg(tmo: Long) = {

      udf((uid: String, clickList: Seq[Long], tsList: Seq[Long]) => {
        var sessionsIdList: ListBuffer[Result] = ListBuffer()

        if (tsList.length <= 1) {

          ListBuffer(Result(uid, clickList(0), 1))
        }

        else {

          val maxSessionTime = tmo
          var entryIndex = 0
          var nextIndex = 1
          var sessionIndex = 0
          var sessionId = 1

          breakable
          {
            for (i <- 0 until tsList.length) {
              //println(i)

              if (entryIndex == tsList.size - 1) {
                if ((tsList(sessionIndex) - tsList(entryIndex)) > maxSessionTime) {
                  sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId + 1))
                }
                else {
                  sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                }
                break //todo: break is not supported
              }

              if ((tsList(nextIndex) - tsList(sessionIndex)) > maxSessionTime) {
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                sessionId += 1
                sessionIndex = nextIndex
                entryIndex += 1
                nextIndex += 1
              }
              else {
                sessionsIdList = sessionsIdList :+ (Result(uid, clickList(entryIndex), sessionId))
                entryIndex += 1
                nextIndex += 1
              }
            }
          }
          (sessionsIdList) //else return statement
        } // end of else
      }) // end of udf
    } // end of function

    val windowSpec = Window.partitionBy("client:port").orderBy("timestamp")

    val r1=df
      .withColumn("uid",concat(lit("u"),lit("_"),dense_rank.over(Window.orderBy("client:port"))).cast("string"))
      .withColumn("row_number", row_number() over windowSpec)
      .withColumn("timestamp_epoch",$"timestamp".cast("long"))
      .withColumn("time_diff",$"timestamp".cast("long") - lag($"timestamp", 1).over(windowSpec).cast("long"))
      .withColumn("time_diff_in_a_frame",$"timestamp".cast("long") - first($"timestamp".cast("long"),true).over(windowSpec))
        .withColumn("time_diff_frame_minute",$"time_diff_in_a_frame"/60)

    val df2 = r1.
      groupBy("uid").agg(
      collect_list($"timestamp_epoch").as("click_list"), collect_list($"time_diff_in_a_frame").as("ts_list")
    ).
      withColumn("click_sess_id",
        //explode(clickSessList(tmo1)($"uid", $"click_list", $"ts_list"))
        explode(myFuncWithArg(tmo1)(
          //struct(r1.col("uid"),r1.col("timestamp_epoch"),r1.col("time_diff_in_a_frame")))
          $"uid", $"click_list", $"ts_list"))
      )
      //.select($"uid", $"click_sess_id._1".as("click_time"), $"click_sess_id._2".as("sess_id"))
        .select($"uid", $"click_sess_id.f_timestamp".as("click_time"), $"click_sess_id.f_sid".as("sess_id"))
    df2.show(50,false)

    //r1.select("row_number","client:port","uid","timestamp","timestamp_epoch","time_diff","time_diff_in_a_frame","time_diff_frame_minute").show(50,false)
  }





  /**
   * row last window
   * @param df
   * @param spark
   */
  def rowsLastWindow(df: DataFrame,spark:SparkSession) = {
    import spark.implicits._

    val win1 = Window.partitionBy($"client:port").orderBy($"timestamp")
    val win2 = Window.partitionBy($"client:port").orderBy($"sessTS")

    val df2 = df.
      withColumn( "firstTS",
        when( row_number.over(win1) === 1 || $"timestamp".cast("long") - lag($"timestamp", 1).over(win1).cast("long") >= tmo1 , 0L)
          .otherwise($"timestamp".cast("long") )
      ).
      withColumn( "sessTS",
        last($"firstTS", ignoreNulls = true).
          over(win1.rowsBetween(Window.unboundedPreceding, 0))
      ).
      withColumn("sessionId", concat(lit("session"), dense_rank.over(win2)))


      df2.select("client:port","timestamp","firstTS","sessionId").orderBy("client:port","timestamp","sessionId").show(50,false)

  }




  /**
   * query based window function
   * @param dataFrame
   * @param spark
   */
   def queryBasedWindows(dataFrame: DataFrame,spark:SparkSession):Unit = {

     val df3 = dataFrame.withColumnRenamed("client:port","client_port")
     df3.createOrReplaceTempView("webdf")

     import spark.implicits._

     val s = spark.sql("""SELECT *, CONCAT(client_port,
      CONCAT('_',SUM(new_session) OVER (PARTITION BY client_port ORDER BY timestamp))
      ) AS session_id
      FROM (SELECT * , CASE WHEN UNIX_TIMESTAMP(timestamp) - LAG (UNIX_TIMESTAMP(timestamp))
      OVER (PARTITION BY client_port ORDER BY timestamp) >= 2 * 60 THEN 1 ELSE 0 END AS new_session FROM webdf ) s1""")

     s.select("client_port","timestamp","session_id").orderBy($"client_port",$"timestamp").show(50,false)
     sys.exit()
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
