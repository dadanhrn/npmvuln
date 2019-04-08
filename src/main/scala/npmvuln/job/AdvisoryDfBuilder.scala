package npmvuln.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object AdvisoryDfBuilder {
  val advisorySchema: StructType = StructType(Array(
    StructField("CWE", StringType, true),
    StructField("Disclosed", DateType, false),
    StructField("Id", StringType, false),
    StructField("Name", StringType, false),
    StructField("Package", StringType, false),
    StructField("Published", DateType, false),
    StructField("References", StringType, false),
    StructField("Severity", StringType, false),
    StructField("Versions", StringType, false)
  ))

  def build(spark: SparkSession, path: String): DataFrame = {
    spark.read

      // Define format
      .format("csv")

      // Define that CSV has header
      .option("header", true)

      // Define format for Date type
      .option("dateFomat", "yyyy-MM-dd")

      // Assign schema
      .schema(this.advisorySchema)

      // Load file
      .load(path)
  }
}
