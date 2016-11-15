package com.crealytics.google.analytics

import java.math.BigDecimal
import java.text.{SimpleDateFormat, NumberFormat}
import java.util.{Calendar, Date, Locale}

import com.google.api.services.analytics.Analytics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.{Try, Success}

case class AnalyticsRelation protected[crealytics](
                                                    analytics: Analytics,
                                                    ids: String,
                                                    startDate: String,
                                                    endDate: String,
                                                    calculatedMetrics: Seq[String],
                                                    queryIndividualDays: Boolean
                                                  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan {

  override val schema: StructType = createSchemaFromColumns(allColumns)

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray, Array())

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array())

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val results = getResults(ids, startDate, endDate, requiredColumns, filters)
    sqlContext.sparkContext.parallelize(results.map(Row.fromSeq))
  }

  private def nDaysAgo(beginDate: Date, n: Integer) = {
    val cal = calendarForDate(beginDate)
    cal.add(Calendar.DATE, n)
    cal.getTime
  }

  val analyticsDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  private def parseGoogleDate(date: String) = {
    if (date == "today") nDaysAgo(new Date(), 0)
    else if (date == "yesterday") nDaysAgo(new Date(), 1)
    else if (date.endsWith("daysAgo")) nDaysAgo(new Date(), date.replace("daysAgo", "").toInt)
    else analyticsDateFormat.parse(date)
  }

  private def calendarForDate(date: Date) = {
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar
  }
  private def getDateRange = {
    val end = calendarForDate(parseGoogleDate(endDate))
    Iterator.iterate(calendarForDate(parseGoogleDate(startDate))) { d =>
      d.add(Calendar.DATE, 1)
      d
    }.takeWhile(!_.after(end)).map(dt => analyticsDateFormat.format(dt.getTime))
  }

  lazy val defaultColumns = analytics.metadata.columns.list("ga").execute.getItems.asScala
  lazy val calculatedMetricTemplate = defaultColumns.find(_.getId == "ga:calcMetric_<NAME>").get
  lazy val allColumns = defaultColumns ++ calculatedMetrics.map(
    name => calculatedMetricTemplate.clone.setId("ga:calcMetric_" + name)
  )

  private def createSchemaFromColumns(columns: Seq[com.google.api.services.analytics.model.Column]) =
    columns.foldLeft(new StructType) {
      case (struct, column) =>
        val attributes = column.getAttributes
        val dataType = sparkDataTypeForGoogleDataType(attributes.get("dataType"))

        val templateBounds: Option[(Int, Int)] = for {
          minTemplateIndex <- Option(attributes.get("minTemplateIndex")).map(_.toInt)
          maxTemplateIndex <- Option(attributes.get("maxTemplateIndex")).map(_.toInt)
        } yield((minTemplateIndex, maxTemplateIndex))

        val columnNames = templateBounds.map { case (minIndex, maxIndex) =>
          (minIndex to maxIndex).map(i => column.getId.replaceFirst("XX", s"$i"))
        }.getOrElse(Seq(column.getId))
        val isDimension = attributes.get("type") == "DIMENSION"
        columnNames.foldLeft(struct) { case(s, c) => s.add(c.replaceFirst("ga:", ""), dataType, nullable = true, metadata = new MetadataBuilder().putBoolean("isDimension", isDimension).build) }
    }

  implicit class RichStructType(st: StructType) {
    def filterByIsDimension(isDimension: Boolean = true): StructType = {
      StructType(st.filter(_.metadata.getBoolean("isDimension") == isDimension))
    }
    def dimensionsAndMetrics: (StructType, StructType) = {
      val (dimensions, metrics) = st.partition(_.metadata.getBoolean("isDimension"))
      (StructType(dimensions), StructType(metrics))
    }
  }
  val (allMetrics, allDimensions) = schema.dimensionsAndMetrics

  private def sparkDataTypeForGoogleDataType(dataType: String) = dataType match {
    case "PERCENT" => "DECIMAL"
    case "CURRENCY" => "DECIMAL"
    case "TIME" => "TIMESTAMP"
    case t => t
  }

  private def castTo(datum: String, castType: DataType): Any = {
    castType match {
      case _: ByteType => datum.toByte
      case _: ShortType => datum.toShort
      case _: IntegerType => datum.toInt
      case _: LongType => datum.toLong
      case _: FloatType => Try(datum.toFloat)
        .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
      case _: DoubleType => Try(datum.toDouble)
        .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
      case _: BooleanType => datum.toBoolean
      case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
      case _: TimestampType => java.sql.Timestamp.valueOf(datum)
      case _: DateType => java.sql.Date.valueOf(datum)
      case _: StringType => datum
      case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
    }
  }

  def combineFilters(filters: Array[Filter]): String = {
    def convertFilter(filter: Filter): String = filter match {
      case EqualTo(attribute, value) => s"ga:$attribute==$value"
      case Not(EqualTo(attribute, value)) => s"ga:$attribute!=$value"
      case EqualNullSafe(attribute, value) => s"ga:$attribute==$value"
      case Not(EqualNullSafe(attribute, value)) => s"ga:$attribute!=$value"
      case GreaterThan(attribute, value) => s"ga:$attribute>$value"
      case GreaterThanOrEqual(attribute, value) => s"ga:$attribute>=$value"
      case LessThan(attribute, value) => s"ga:$attribute<$value"
      case LessThanOrEqual(attribute, value) => s"ga:$attribute<=$value"
      case In(attribute, values) => s"ga:$attribute[]${values.mkString("|")}"
      case And(lhs, rhs) => Seq(lhs, rhs).map(convertFilter).mkString(";")
      case Or(lhs, rhs) => Seq(lhs, rhs).map(convertFilter).mkString(",")
      case StringStartsWith(attribute, value) => s"ga:$attribute=~^$value"
      case StringEndsWith(attribute, value) => s"ga:$attribute=~$value$$"
      case StringContains(attribute, value) => s"ga:$attribute=@$value"
      case IsNull(attribute) => ???
      case IsNotNull(attribute) => ???
      case Not(_) => ???
    }
    filters.map(convertFilter).mkString(";")
  }

  @annotation.tailrec
  private final def retry[T](n: Int)(fn: => T): Try[T] = {
    Try(fn) match {
      case x: Success[T] => x
      case _ if n > 1 => retry(n - 1)(fn)
      case f => f
    }
  }

  private def getResults(ids: String, startDate: String, endDate: String,
                         requiredColumns: Seq[String], filters: Array[Filter]): Seq[Seq[Any]] = {
    val requiredSchema = StructType(requiredColumns.map(c => schema.find(_.name == c).get))
    val (requiredDimensions, rawMetrics) = requiredSchema.dimensionsAndMetrics
    if (queryIndividualDays && !requiredDimensions.map(_.name).contains("date")) {
      throw new IllegalArgumentException("If you use queryIndividualDays, you must select the date dimension.")
    }
    val requiredMetrics = if (rawMetrics.nonEmpty) rawMetrics
    // We need at least 1 metric, otherwise Google complains
    else Seq[StructField](allMetrics.head)
    val maxPageSize = 10000
    val filtersString = combineFilters(filters)

    def queryDateRange(startDate: String, endDate: String) = {
      val queryWithoutFilter = analytics.data().ga()
        .get(ids, startDate, endDate, requiredMetrics.map("ga:" + _.name).mkString(","))
        .setDimensions(requiredDimensions.map("ga:" + _.name).mkString(","))
        .setMaxResults(maxPageSize)
      val query = if (filters.length > 0) queryWithoutFilter.setFilters(filtersString) else queryWithoutFilter
      val firstResult = query.execute
      val requiredPages = firstResult.getTotalResults / maxPageSize
      val restResults = (1 to requiredPages).flatMap { pageNum =>
        retry(3)(query.setStartIndex(pageNum * maxPageSize).execute.getRows.asScala).get
      }
      val columnHeaders = firstResult.getColumnHeaders.asScala
      val firstRows = Option(firstResult.getRows).getOrElse(java.util.Collections.emptyList).asScala
      val combinedResult = (firstRows ++ restResults).map(_.asScala)
      val maps = combinedResult.map { line =>
        columnHeaders.zip(line).flatMap { case (header, cell) =>
          val name = header.getName.replaceFirst("ga:", "")
          if (requiredColumns.contains(name)) {
            val dataType = requiredSchema.apply(name).dataType
            Some(name -> castTo(cell, dataType))
          } else None
        }.toMap
      }
      maps.map(m => requiredColumns.map(m(_)))
    }

    if (queryIndividualDays) {
      getDateRange.map(date => queryDateRange(date, date)).reduce(_ union _)
    } else {
      queryDateRange(startDate, endDate)
    }
  }
}
