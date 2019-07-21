package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object ScannedDfBuilder {

  // Schema definition for scannedDf CSV
  val vulnDfSchema: StructType = StructType(Array(
    StructField("Id", StringType, false),
    StructField("Project", StringType, false),
    StructField("Release", StringType, false),
    StructField("Name", StringType, false),
    StructField("Severity", StringType, false),
    StructField("StartDate", TimestampType, false),
    StructField("EndDate", TimestampType, false),
    StructField("Duration", IntegerType, false),
    StructField("Level", IntegerType, false)
  ))

  def build(spark: SparkSession, path: String): DataFrame = {
    spark.read

      // Define format
      .format("csv")

      // Define that CSV has header
      .option("header", false)

      // Define format for Timestamp type
      .option("timestampFormat", "yyyy-MM-dd hh:mm:ss z")

      // Assign schema
      .schema(vulnDfSchema)

      // Load file
      .load(path)
  }
}
