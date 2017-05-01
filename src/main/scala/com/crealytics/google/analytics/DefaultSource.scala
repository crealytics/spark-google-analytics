package com.crealytics.google.analytics

import java.io.File
import java.util

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.analytics.{Analytics, AnalyticsScopes}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import com.google.api.client.auth.oauth2.Credential

class DefaultSource
  extends RelationProvider {

  val jsonFactory = GsonFactory.getDefaultInstance
  val httpTransport = GoogleNetHttpTransport.newTrustedTransport()

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): AnalyticsRelation = {
    val credentialsFromSecretAndToken = for {
      clientId <- parameters.get("clientId")
      clientSecret <- parameters.get("clientSecret")
      refreshToken <- parameters.get("refreshToken")
    } yield {
      val credentials = new GoogleCredential.Builder().setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setClientSecrets(clientId, clientSecret)
        .build()
      credentials.setRefreshToken(refreshToken)
      credentials
    }

    val credentialsFromKeyFile = for {
      keyFileLocation <- parameters.get("keyFileLocation")
      serviceAccountId <- parameters.get("serviceAccountId")
    } yield {
      new GoogleCredential.Builder().setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(serviceAccountId)
        .setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
        .setServiceAccountScopes(util.Arrays.asList(AnalyticsScopes.ANALYTICS_READONLY))
        .build()
    }

    val credentials = credentialsFromSecretAndToken
      .orElse(credentialsFromKeyFile)
      .getOrElse(
        throw new Exception("Please provide valid credentials information. See README file for more info."))
    val analytics = new Analytics.Builder(httpTransport, jsonFactory, credentials)
      .setApplicationName("spark-google-analytics")
      .build()
    val calculatedMetrics = parameters.getOrElse("calculatedMetrics", "").split(",").map(_.trim)
    val queryIndividualDays: Boolean = parameters.getOrElse("queryIndividualDays", "false") == "true"
    AnalyticsRelation(
      analytics,
      parameters("ids"),
      parameters("startDate"),
      parameters("endDate"),
      calculatedMetrics,
      queryIndividualDays
    )(sqlContext)
  }
}
