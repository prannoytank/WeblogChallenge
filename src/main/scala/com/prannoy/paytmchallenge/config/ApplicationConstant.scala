package com.prannoy.paytmchallenge.config

object ApplicationConstant {

  lazy val FilePathKey = "app.filePath"
  lazy val MaxSessionTimeKey="app.maxSessionTime"
  lazy val RunSequenceKey = "app.runSequence"


  lazy val HelpMsg =
    s"""
        spark-submit application options options are:
          -h | --help      Show this message and exit.
          --filePath      Path of file
          --maxSessionTime Max Session Time / session cutoff window
          --runSequence ["ETL","ML"]
        """

}
