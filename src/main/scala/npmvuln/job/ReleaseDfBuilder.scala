package npmvuln.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lag, monotonically_increasing_id, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import com.github.zafarkhaja.semver.Version

object ReleaseDfBuilder {

  // Schema definition for libio-versions.csv
  val libioVersionsSchema: StructType = StructType(Array(
    StructField("Project", StringType, false),
    StructField("Release", StringType, false),
    StructField("Date", TimestampType, false)
  ))

  def build(spark: SparkSession, path: String, advisoryDf: DataFrame): DataFrame = {

    // Define version constraint comparation UDF
    val checkConstraint: UserDefinedFunction = udf[Boolean, String, String]((version, constraint) => {
      Version.valueOf(version).satisfies(constraint)
    })

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

      // Add ID field for every release (-1 downwards)
      .withColumn("ReleaseId", (monotonically_increasing_id + 1) * -1)

      // Add column for date of next release
      .withColumn("NextReleaseDate",
        lag("Date", -1, null)
          .over(Window.partitionBy("Project").orderBy("Date")))

      // Join with advisory dataframe
      .join(advisoryDf,
        (col("Project") === advisoryDf("Package")) &&
          (checkConstraint(col("Release"), advisoryDf("Versions"))),
        "left_outer")
  }

}
