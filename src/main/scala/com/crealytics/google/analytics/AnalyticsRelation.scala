package com.crealytics.google.analytics

import com.google.api.services.analytics.Analytics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.Try

case class AnalyticsRelation protected[crealytics](
  analytics: Analytics,
  ids: String,
  startDate: String,
  endDate: String,
  dimensions: Seq[String],
  metrics: Seq[String]
  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan {

  override val schema: StructType = getColumnsFromNames(dimensions ++ metrics)

  override def buildScan: RDD[Row] = buildScan(schema.map(_.name).toArray, Array())

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array())

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val results = getResults(ids, startDate, endDate,
      dimensions.filter(requiredColumns.contains), metrics.filter(requiredColumns.contains), filters)
    sqlContext.sparkContext.parallelize(results.map(Row.fromSeq))
  }

  private def getColumnsFromNames(columns: Seq[String]) =
    columns.map(completeSchema.apply).foldLeft(new StructType)(_ add _)

  lazy val completeSchema = analytics.metadata.columns.list("ga").execute.getItems.asScala.foldLeft(new StructType) {
    case (struct, column) =>
      val attributes = column.getAttributes
      val dataType = sparkDataTypeForGoogleDataType(attributes.get("dataType"))
      struct.add(column.getId.replaceFirst("ga:", ""), dataType)
  }

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
      case Not(filter) => ???
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
    dimensions: Seq[String], metrics: Seq[String], filters: Array[Filter]): Seq[Seq[Any]] = {
    val maxPageSize = 10000
    val filtersString = combineFilters(filters)
    val queryWithoutFilter = analytics.data().ga().get(ids, startDate, endDate, metrics.map("ga:" + _).mkString(","))
      .setDimensions(dimensions.map("ga:" + _).mkString(","))
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
      columnHeaders.zip(line).map { case (header, cell) =>
        val dataType = completeSchema.apply(header.getName.replaceFirst("ga:", "")).dataType
        castTo(cell, dataType)
      }
    }
  }
}
