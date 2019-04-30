package npmvuln.job

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession, Row, Dataset}
import org.apache.spark.sql.types._
import java.sql.Timestamp
import npmvuln.props._

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
    StructField("Level", IntegerType, true)
  ))

  def run(spark: SparkSession, resultGraph: Graph[VertexProperties, EdgeProperties]): DataFrame = {

    // Get vertices
    val resultRDD = resultGraph.vertices

      // Filter to get only PackageState vertices
      .filter(_._2.isInstanceOf[PackageStateVertex])

      // Convert VertexProperties to PackageStateVertex
      .map(_.asInstanceOf[PackageStateVertex])

      // Filter out PackageState unaffected by vulnerability
      .filter(!_.vulnRecords.isEmpty)

      // Build dataframe rows
      .flatMap(rel => {
        rel.vulnRecords.map(vuln => {
          Row(vuln.id, vuln.name, vuln.severity, rel.packageName, rel.version,
            Timestamp.from(vuln.period.getStart), Timestamp.from(vuln.period.getEnd))
        })
      })

    // Build dataframe
    spark.createDataFrame(resultRDD, schema)
  }
}
