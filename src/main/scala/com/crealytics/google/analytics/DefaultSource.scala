package com.crealytics.google.analytics


import java.io.File
import java.util

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.analytics.{Analytics, AnalyticsScopes}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class DefaultSource
  extends RelationProvider {

  val jsonFactory = GsonFactory.getDefaultInstance
  val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): AnalyticsRelation = {
    val keyFileLocation = parameters("keyFileLocation")
    val serviceAccountId = parameters("serviceAccountId")
    val credential = new GoogleCredential.Builder().setTransport(httpTransport)
      .setJsonFactory(jsonFactory)
      .setServiceAccountId(serviceAccountId)
      .setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
      .setServiceAccountScopes(util.Arrays.asList(AnalyticsScopes.ANALYTICS_READONLY))
      .build()
    val analytics = new Analytics.Builder(httpTransport, jsonFactory, credential)
      .setApplicationName("spark-google-analytics")
      .build()
    AnalyticsRelation(
      analytics,
      parameters("ids"),
      parameters("startDate"),
      parameters("endDate"),
      parameters("dimensions").split(",").map(_.trim),
      parameters.getOrElse("queryIndividualDays", "false") == "true"
    )(sqlContext)
  }
}
