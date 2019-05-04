package npmvuln.stats

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, sum, when}
import npmvuln.helpers.CumulativeProduct

object KaplanMeierEstimator {

  def build(resultDf: DataFrame): DataFrame = {

    // Get number of vulnerabilities
    val individuals: Long = resultDf.count

    // Source dataframe
    resultDf

      // Group by duration of vulnerability then by censor
      .groupBy(col("Duration"), col("Uncensored"))

      // Get number of event (vulnerability) occurences in each group
      .count
      .withColumnRenamed("count", "Count")

      // Get number of observed (uncensored) event
      .withColumn("Event",
        when(col("Uncensored") === true, col("Count"))
          .otherwise(0))

      // Get cumulative frequency of all event
      .withColumn("CumulativeEvent",
        sum("Count")
          .over(Window.orderBy(col("Duration").asc)
            .rowsBetween(Long.MinValue, 0)))

      // Get number of survivor just prior to current time
      .withColumn("Survivor",
        (lag("CumulativeEvent", 1, 0)
          .over(Window.orderBy("Duration")) - individuals) * -1)

      // Calculate factor
      .withColumn("Factor",
        (col("Survivor") - col("Event")) / col("Survivor"))

      // Calculate values in survival function
      .withColumn("SurvivalFunction",
        CumulativeProduct(col("Factor"))
          .over(Window.orderBy(col("Duration"))
            .rowsBetween(Long.MinValue, 0)))

  }
}