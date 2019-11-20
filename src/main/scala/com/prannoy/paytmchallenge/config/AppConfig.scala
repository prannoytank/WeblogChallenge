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

  private var fileConfPathObj: List[String] = List.empty;

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

    /**
     * function to parse only the file config paths passed as arguments
     */
    args.sliding(2, 2).toList.collect {
      case Array("--configFilePathString", filePathArg: String) => fileConfPathObj = createFileConfigPath(filePathArg)
    }


    val appConfig = ConfigFactory.parseFile(parseApplicationConfigFiles)

    val config = ConfigFactory.parseMap(argsMap.asJava).withFallback(appConfig).resolve()
    appEntityObj = ConfigBeanFactory.create(config.getConfig("app"), classOf[AppEntity])

  }

  private def parseApplicationConfigFiles(): File ={
    var appFile: File = null

    if (fileConfPathObj.isEmpty || fileConfPathObj == null) {
      appFile = new File(getClass.getResource("/application.conf").getPath)
    }

    else if (fileConfPathObj.length >= 1 ) {
      for (file <- fileConfPathObj) {
        file.split("/").last match {
          case s if s.contains("application.conf") => {
            println("Setting up app conf")
            appFile = new File(file)
          }
        }
      }
    }
    //    else {
    //      println("Need at-least 1 configuration file (application.conf,env.conf,workflow.conf)")
    //      sys.exit(1)
    //    }


    if(appFile == null) {
      println("Cannot parse the application config file.Exiting")
      sys.exit(1)
    }

    appFile
  }

  private def createFileConfigPath(jsonString: String): List[String] = {
    jsonString.split(",").map(_.trim).toList
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
