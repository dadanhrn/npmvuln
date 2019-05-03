package npmvuln.jobs

import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DependenciesDfBuilder {

  // Define schema for libio-dependencies.csv
  val libioDepedenciesSchema: StructType = StructType(Array(
    StructField("Project", StringType, false),
    StructField("Release", StringType, false),
    StructField("Dependency", StringType, false),
    StructField("Constraint", StringType, false)
  ))

  def build(spark: SparkSession, path: String): DataFrame = {
    spark.read

      // Define format
      .format("csv")

      // Define that CSV has header
      .option("header", true)

      // Assign schema
      .schema(this.libioDepedenciesSchema)

      // Load file
      .load(path)

      // Trim string values
      .withColumn("Project", trim(col("Project")))
      .withColumn("Release", trim(col("Release")))
      .withColumn("Dependency", trim(col("Dependency")))
      .withColumn("Constraint", trim(col("Constraint")))
  }

}
