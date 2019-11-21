# WeblogChallenge
This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Description

1. Maven based project (3.6)
2. Java 1.8 , scala 2.11.8
3. Built and tested on Cloudera Quickstart VM (5.13)

#### Libraries used
1. Type safe config to read the configuration file
2. Spark libraries like core,hive,sql,ml


## Command Line args for the application
1. --filePath  hdfs://cloudera.quickstart:8020/user/cloudera/sampleFile.csv
2. --runSequence Example: ["ETL",ML]
3. --maxSessionTime 900  (value in seconds)

## Usage
IDE
```$xslt
If Running via IDE(Intellij) , directly run the application. 
application.conf would be referenced from the resource directory. So you change the parameters from the application.conf itself
```

## application.conf

All the three properties inside the file are optional.

If you dont pass anything , the default value would be:
1. filePath = "file:///[your project directory]/data/2015_07_22_mktplace_shop_web_log_sample.log.gz";
2. maxSessionTime = 900
3. runSequence=["ETL"]

```$xslt
app = {
#filePath = xy 
runSequence=["etl","ML"] 
maxSessionTime=900
}
```

##### Note: Processing and Analytics goals has been done. MLE questions are pending
