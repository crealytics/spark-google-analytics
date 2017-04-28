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
   
    /*** Local initialization ***/
    
    /*** Authentication Method Field Set - 1 */
    //Client ID
    var clientId: String = null
    
    //Client Secret
    var clientSecret: String = null
    
    //Refresh Token
    var refreshToken: String = null

    /*** Authentication Method Field Set - 2 */
    //Service Account ID
    var serviceAccountId: String = null
    
    //Key File Location (.p12)
    var keyFileLocation: String = null

    //Credential Object 
    var credential: Credential = null

    println("Building Credentials")

    if (parameters.contains("clientId") && parameters.contains("clientSecret") && parameters.contains("refreshToken")) {

      //Build Credential Object
      println("Credentials will be build based on Clinet ID, Client Secret and Refresh Token")

      clientId = parameters("clientId")
      clientSecret = parameters("clientSecret")
      refreshToken = parameters("refreshToken")

      credential = new GoogleCredential.Builder().setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setClientSecrets(clientId, clientSecret)
        .build()

      credential.setRefreshToken(refreshToken)

    } else if (parameters.contains("serviceAccountId") && parameters.contains("keyFileLocation")) {

      //Build Credential Object
      println("Credentials will be build based on Service Account ID and Key File Location")

      keyFileLocation = parameters("keyFileLocation")
      serviceAccountId = parameters("serviceAccountId")
      
      credential = new GoogleCredential.Builder().setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(serviceAccountId)
        .setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
        .setServiceAccountScopes(util.Arrays.asList(AnalyticsScopes.ANALYTICS_READONLY))
        .build()
    } else {

      println("Incomplete credential information. Please provide valid credentials.")
      throw new Exception("Incomplete credential information. Please provide valid credentials. See README file instructions for more info.")

    }

    println("Building Analytics Object")

    //Build Analytics Object
    val analytics = new Analytics.Builder(httpTransport, jsonFactory, credential)
      .setApplicationName("spark-google-analytics")
      .build()

    //Calculated Metrics
    val calculatedMetrics = parameters.getOrElse("calculatedMetrics", "").split(",").map(_.trim)
    
    //Query Individual Days (Boolean)
    val queryIndividualDays: Boolean = parameters.getOrElse("queryIndividualDays", "false") == "true"

    //Call to AnalyticsRelation
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
