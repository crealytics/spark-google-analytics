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
version: 1.1.0
```
### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-google-analytics_2.11
version: 1.1.0
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-google-analytics_2.11:1.1.0
```

### Spark compiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-google-analytics_2.10:1.1.0
```

## Features
This package allows querying Google Analytics reports as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).
The API accepts several options (see the [Google Analytics developer docs](https://developers.google.com/analytics/devguides/reporting/core/v3/reference#q_details) for details):
* `serviceAccountId`: an account id for accessing the Google Analytics API (`xxx-xxxxxxx@developer.gserviceaccount.com`)
* `keyFileLocation`: a key-file that you have to generate from the developer console
* `clientId`: an account id that you have to generate from the developer console using OAuth 2.0 credentials option
* `clientSecret`: a client secret id that you have to obtain from the developer console for OAuth 2.0 credentials client id which you have already generated
* `refreshToken`: a refresh token is need to be obtained by User's Login for which you wanted to collect GA data. Once user login from appropriate call you will get this token in response [OAuth2WebServer Offline](https://developers.google.com/identity/protocols/OAuth2WebServer#offline)
* `ids`: the ID of the site for which you want to pull the data
* `startDate`: the start date for the report
* `endDate`: the end date for the report
* `queryIndividualDays`: fetches each day from the chosen date range individually in order to minimize sampling (only works if `date` is chosen as dimension)
* `calculatedMetrics`: the suffixes of any calculated metrics (defined in your GA view) you want to query

### Scala API
__Spark 1.4+:__

```scala
import org.apache.spark.sql.SQLContext
```
Option 1 : Authentication with Service Account ID and P12 Key File

```scala
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.crealytics.google.analytics")
    .option("serviceAccountId", "xxx-xxxxxxx@developer.gserviceaccount.com")
    .option("keyFileLocation", "the_key_file.p12")
    .option("ids", "ga:12345678")
    .option("startDate", "7daysAgo")
    .option("endDate", "yesterday")
    .option("queryIndividualDays", "true")
    .option("calculatedMetrics", "averageEngagement")
    .load()
```
OR 

Option 2 : Authentication with Client ID, Client Secret and Refresh Token'

```scala
val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.crealytics.google.analytics")
    .option("clientId", "XXXXXXXX-xyxyxxxxyxyxxxxxyyyx.apps.googleusercontent.com")
    .option("clientSecret", "73xxYxyxy-XXXYZZx-xZ_Z")
    .option("refreshToken", "1/ezzzxZYzxxyyXYXzyyXXYYyxxxxyyyyxxxy")
    .option("ids", "ga:12345678")
    .option("startDate", "7daysAgo")
    .option("endDate", "yesterday")
    .option("queryIndividualDays", "true")
    .option("calculatedMetrics", "averageEngagement")
    .load()


// You need select the date column if using queryIndividualDays
df.select("date", "browser", "city", "users", "calcMetric_averageEngagement").show()
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
