package npmvuln.job

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.monotonically_increasing_id

object ProjectDfBuilder {

  def build(libioVersionsDf: DataFrame): DataFrame = {
    libioVersionsDf

      // Select column Project
      .select("Project")

      // Select distinct package names
      .distinct

      // Add ID field for every package (0 upwards)
      .withColumn("ProjectId", monotonically_increasing_id)
  }
}
