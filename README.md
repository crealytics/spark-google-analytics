# Spark Google Analytics Library

A library for querying Google Analytics data with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://travis-ci.org/crealytics/spark-google-analytics.svg?branch=master)](https://travis-ci.org/crealytics/spark-google-analytics)

## Requirements

This library requires Spark 1.4+

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.10
```
groupId: com.crealytics
artifactId: spark-google-analytics_2.10
version: 0.8.2
```
### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-google-analytics_2.11
version: 0.8.2
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-google-analytics_2.11:0.8.2
```

### Spark compiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-google-analytics_2.10:0.8.2
```

## Features
This package allows querying Google Analytics reports as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).
The API accepts several options (see the [Google Analytics developer docs](https://developers.google.com/analytics/devguides/reporting/core/v3/reference#q_details) for details):
* `serviceAccountId`: an account id for accessing the Google Analytics API (`xxx-xxxxxxx@developer.gserviceaccount.com`)
* `keyFileLocation`: a key-file that you have to generate from the developer console
* `ids`: the ID of the site for which you want to pull the data
* `startDate`: the start date for the report
* `endDate`: the end date for the report
* `dimensions`: the dimensions by which the data will be segmented

### Scala API
__Spark 1.4+:__

Automatically infer schema (data types), otherwise everything is assumed string:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.crealytics.google.analytics")
    .option("serviceAccountId", "xxx-xxxxxxx@developer.gserviceaccount.com")
    .option("keyFileLocation", "the_key_file.p12")
    .option("ids", "ga:12345678")
    .option("startDate", "7daysAgo")
    .option("endDate", "yesterday")
    .option("dimensions", "browser,city")
    .load()
    
df.select("browser", "users").show()
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
