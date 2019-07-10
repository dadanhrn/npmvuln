package npmvuln.jobs

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.time.{Duration, Instant}
import npmvuln.props._
import npmvuln.helpers.constants.CENSOR_DATE

object ResultDfBuilder {

  // Define dataframe schema
  val schema: StructType = StructType(Array(
    StructField("Id", StringType, false),
    StructField("Name", StringType, false),
    StructField("Severity", StringType, false),
    StructField("Package", StringType, false),
    StructField("Release", StringType, false),
    StructField("Since", TimestampType, false),
    StructField("To", TimestampType, false),
    StructField("Duration", LongType, false),
    StructField("Uncensored", BooleanType, false),
    StructField("Level", IntegerType, false)
  ))

  def run(spark: SQLContext, resultGraph: Graph[VertexProperties, EdgeProperties]): DataFrame = {

    // Get vertices
    val resultRDD = resultGraph.vertices

      // Filter to get only PackageState vertices
      .filter(_._2.isInstanceOf[PackageStateVertex])

      // Convert VertexProperties to PackageStateVertex
      .map(_._2.asInstanceOf[PackageStateVertex])

      // Filter out PackageState unaffected by vulnerability
      .filter(!_.vulnRecords.isEmpty)

      // Build dataframe rows
      .flatMap(rel => {
        rel.vulnRecords.map(vuln => {
          val affected_since: Instant = vuln.period.getStart
          val affected_to: Instant = vuln.period.getEnd
          val affected_duration: Long = Duration.between(affected_since, affected_to).toDays
          val isUncensored: Boolean = affected_to != CENSOR_DATE

          Row(vuln.id, vuln.name, vuln.severity, rel.packageName, rel.version,
            Timestamp.from(affected_since), Timestamp.from(affected_to), affected_duration, isUncensored, vuln.level)
        })
      })

    // Build dataframe
    spark.createDataFrame(resultRDD, schema)
  }
}
