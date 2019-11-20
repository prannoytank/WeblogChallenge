package com.prannoy.paytmchallenge.services.TaskRunner

import com.prannoy.paytmchallenge.config.AppConfig

trait TaskRunnerTrait {
  protected final val appEntityObj = AppConfig.getAppEntityObj
  def run()
}