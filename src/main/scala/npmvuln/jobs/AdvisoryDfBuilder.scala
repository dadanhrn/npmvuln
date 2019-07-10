package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, trim}

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

  def build(spark: SQLContext, path: String): DataFrame = {
    spark.read

      // Define format
      .format("csv")

      // Define that CSV has header
      .option("header", "true")

      // Define format for Date type
      .option("timestampFormat", "yyyy-MM-dd")

      // Assign schema
      .schema(this.advisorySchema)

      // Load file
      .load(path)

      // Trim string values
      .withColumn("CWE", trim(col("CWE")))
      .withColumn("Id", trim(col("Id")))
      .withColumn("Name", trim(col("Name")))
      .withColumn("Package", trim(col("Package")))
      .withColumn("References", trim(col("References")))
      .withColumn("Severity", trim(col("Severity")))
      .withColumn("Versions", trim(col("Versions")))

      // Remove malicious packages
      .filter(col("Name") !== "Malicious Package")
      .filter(col("Versions") !== "*")

  }
}
