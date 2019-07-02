package npmvuln.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object ProjectDfBuilder {

  def build(libioVersionsDf: DataFrame): DataFrame = {
    libioVersionsDf

      // Select column Project
      .select("Project")

      // Select distinct package names
      .distinct

      // Add ID field for every package (0 upwards)
      .withColumn("ProjectId",
        row_number.over(Window.orderBy("Project")).cast(LongType) - 1)
  }
}
