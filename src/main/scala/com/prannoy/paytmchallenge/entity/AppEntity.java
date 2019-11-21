package com.prannoy.paytmchallenge.entity;

import com.typesafe.config.Optional;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class AppEntity {

    private static AppEntity single_instance = null;

    @Optional
    private String filePath ="file:///"+System.getProperty("user.dir")+"/data/2015_07_22_mktplace_shop_web_log_sample.log.gz";

    @Optional
    private long maxSessionTime = 15*60;

    @Optional
    private List<String> runSequence = Arrays.asList("ETL");

    public static AppEntity getInstance()
    {
        if (single_instance == null)
            single_instance = new AppEntity();

        return single_instance;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getMaxSessionTime() {
        return maxSessionTime;
    }

    public void setMaxSessionTime(long maxSessionTime) {
        this.maxSessionTime = maxSessionTime;
    }

    public List<String> getRunSequence() {
        return runSequence;
    }

    public void setRunSequence(List<String> runSequence) {
        this.runSequence = runSequence;
    }

    @Override
    public String toString() {
        return "AppEntity{" +
                "filePath='" + filePath + '\'' +
                ", maxSessionTime=" + maxSessionTime +
                ", runSequence=" + runSequence +
                '}';
    }
}
