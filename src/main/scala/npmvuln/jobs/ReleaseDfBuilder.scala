package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, trim, lag, monotonically_increasing_id, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import java.sql.Timestamp
import npmvuln.helpers.constants.CENSOR_DATE

object ReleaseDfBuilder {

  // Schema definition for libio-versions.csv
  val libioVersionsSchema: StructType = StructType(Array(
    StructField("Project", StringType, false),
    StructField("Release", StringType, false),
    StructField("Date", TimestampType, false)
  ))

  def build(spark: SparkSession, path: String): DataFrame = {

    // Define format
    spark.read
      .format("csv")

      // Define that CSV has header
      .option("header", true)

      // Define format for Timestamp type
      .option("timestampFormat", "yyyy-MM-dd hh:mm:ss z")

      // Assign schema
      .schema(this.libioVersionsSchema)

      // Load file
      .load(path)

      // Trim string values
      .withColumn("Project", trim(col("Project")))
      .withColumn("Release", trim(col("Release")))

      // Add ID field for every release (-1 downwards)
      .withColumn("ReleaseId", (monotonically_increasing_id + 1) * -1)

      // Add column for date of next release
      .withColumn("NextReleaseDate",
        lag("Date", -1, Timestamp.from(CENSOR_DATE))
          .over(Window.partitionBy("Project").orderBy("Date")))

  }

}
