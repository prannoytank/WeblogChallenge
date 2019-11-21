package com.prannoy.paytmchallenge.config

import java.io.File

import com.prannoy.paytmchallenge.entity.AppEntity
import com.typesafe.config.{ConfigBeanFactory, ConfigFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object AppConfig {

  @transient private lazy val logger = LogManager.getLogger(this.getClass)


  private var appEntityObj: AppEntity = null;

  //def getSparkObj: SparkSession = this.spark


  def getAppEntityObj: AppEntity = this.appEntityObj
  /**
   * Main function to parse application.conf
   * @param args
   */
  def parseApplicationConfig(args: Array[String]) {
    val argsMap = parseArgs(args).getOrElse {
      println(ApplicationConstant.HelpMsg)
      sys.exit(1)
    }

    val appConfig = ConfigFactory.parseFile(new File(getClass.getResource("/application.conf").getPath))

    val config = ConfigFactory.parseMap(argsMap.asJava).withFallback(appConfig).resolve()
    appEntityObj = ConfigBeanFactory.create(config.getConfig("app"), classOf[AppEntity])

  }

  /**
   * Parse command line args
   * @param args : seq of arguments
   * @return map of key value pairs
   */
  private def parseArgs(args: Seq[String]): Try[Map[String, Object]] =
    args match {
      case ("-h" | "--help") +: _ =>
        println(args)
        Failure(new Exception("show help"))
      case "--filePath" +: filePath +: tail =>
        parseArgs(tail).map(m => m + (ApplicationConstant.FilePathKey -> filePath))
      case "--maxSessionTime" +: maxSessionTime +: tail =>
        parseArgs(tail).map(m => m + (ApplicationConstant.MaxSessionTimeKey -> maxSessionTime))
      case "--runSequence" +: runSequence +: tail =>
        parseArgs(tail).map(m => m + (ApplicationConstant.RunSequenceKey -> runSequence))
      case head +: tail =>
        parseArgs(tail) // unknown "head" is ignored
      case Nil =>
        Success(Map.empty[String, Object])
    }
}
