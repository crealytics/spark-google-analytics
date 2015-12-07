package com.crealytics.google.analytics

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale

import com.google.api.services.analytics.Analytics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.util.Try

case class AnalyticsRelation protected[crealytics](
                                                    analytics: Analytics,
                                                    ids: String,
                                                    startDate: String,
                                                    endDate: String,
                                                    dimensions: Seq[String]
                                                  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan {

  override val schema: StructType = new StructType(allMetrics.fields ++ allDimensions.fields)

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray, Array())

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array())

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val results = getResults(ids, startDate, endDate, requiredColumns, filters)
    sqlContext.sparkContext.parallelize(results.map(Row.fromSeq))
  }

  lazy val allColumns = analytics.metadata.columns.list("ga").execute.getItems.asScala

  private def createSchemaFromColumns(columns: Seq[com.google.api.services.analytics.model.Column]) =
    columns.foldLeft(new StructType) {
      case (struct, column) =>
        val attributes = column.getAttributes
        val dataType = sparkDataTypeForGoogleDataType(attributes.get("dataType"))
        struct.add(column.getId.replaceFirst("ga:", ""), dataType)
    }

  lazy val allMetrics = createSchemaFromColumns(allColumns.filter(_.getAttributes.get("type") == "METRIC"))
  lazy val allDimensions = createSchemaFromColumns(allColumns.filter(
    c => c.getAttributes.get("type") == "DIMENSION" &&
      dimensions.contains(c.getId.replaceFirst("ga:", ""))))



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
      case _: TimestampType => Timestamp.valueOf(datum)
      case _: DateType => Date.valueOf(datum)
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
  private final def retry[T](n: Int)(fn: => T): util.Try[T] = {
    util.Try(fn) match {
      case x: util.Success[T] => x
      case _ if n > 1 => retry(n - 1)(fn)
      case f => f
    }
  }

  private def getResults(ids: String, startDate: String, endDate: String,
                         requiredColumns: Seq[String], filters: Array[Filter]): Seq[Seq[Any]] = {
    val rawMetrics = requiredColumns.filterNot(dimensions.contains)
    val requiredMetrics = if (rawMetrics.nonEmpty) rawMetrics
      // We need at least 1 metric, otherwise Google complains
      else Seq[String](allMetrics.head.name.replaceFirst("ga:", ""))
    val metricsSchema = allMetrics.filter( m => requiredMetrics.contains(m.name))
    val completeSchema = new StructType(metricsSchema.toArray ++ allDimensions.fields)
    val maxPageSize = 10000
    val filtersString = combineFilters(filters)
    val queryWithoutFilter = analytics.data().ga()
      .get(ids, startDate, endDate, requiredMetrics.map("ga:" + _).mkString(","))
      .setDimensions(allDimensions.map("ga:" + _.name).mkString(","))
      .setMaxResults(maxPageSize)
    val query = if (filters.length > 0) queryWithoutFilter.setFilters(filtersString) else queryWithoutFilter
    val firstResult = query.execute
    val requiredPages = firstResult.getTotalResults / maxPageSize
    val restResults = (1 to requiredPages).flatMap { pageNum =>
      retry(3)(query.setStartIndex(pageNum * maxPageSize).execute.getRows.asScala).get
    }
    val columnHeaders = firstResult.getColumnHeaders.asScala
    val combinedResult = (firstResult.getRows.asScala ++ restResults).map(_.asScala)
    combinedResult.map { line =>
      columnHeaders.zip(line).flatMap { case (header, cell) =>
        val name = header.getName.replaceFirst("ga:", "")
        if (requiredColumns.contains(name)) {
          val dataType = completeSchema.apply(name).dataType
          Some(castTo(cell, dataType))
        } else None
      }
    }
  }
}
