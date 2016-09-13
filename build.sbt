name := "spark-google-analytics"

version := "1.0.0"

organization := "com.crealytics"

scalaVersion := "2.11.8"

spName := "crealytics/spark-google-analytics"

crossScalaVersions := Seq("2.10.6", "2.11.8")

sparkVersion := "2.0.0"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "com.google.apis" % "google-api-services-analytics" % "v3-rev134-1.22.0",
  "com.google.http-client" % "google-http-client-gson" % "1.22.0"
)

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra :=
  <url>https://github.com/crealytics/spark-google-analytics</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:crealytics/spark-google-analytics.git</url>
    <connection>scm:git:git@github.com:crealytics/spark-google-analytics.git</connection>
  </scm>
  <developers>
    <developer>
      <id>nightscape</id>
      <name>Martin Mauch</name>
      <url>http://www.crealytics.com</url>
    </developer>
  </developers>

// Skip tests during assembly
test in assembly := {}

// -- MiMa binary compatibility checks ------------------------------------------------------------

import com.typesafe.tools.mima.plugin.MimaKeys.{binaryIssueFilters, previousArtifact}
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings

mimaDefaultSettings ++ Seq(
  previousArtifact := Some("com.crealytics" %% "spark-google-analytics" % "0.8.0"),
  binaryIssueFilters ++= Seq(
  )
)

// ------------------------------------------------------------------------------------------------
