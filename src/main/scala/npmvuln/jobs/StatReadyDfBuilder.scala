package npmvuln.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{datediff, col, floor, lit, max, min, udf, when}
import java.sql.Timestamp
import npmvuln.helpers.constants.CENSOR_DATE

object StatReadyDfBuilder {
  def build(resultDf: DataFrame): DataFrame = {

    // Source dataframe
    resultDf

      // Group aggregation by vulnerability ID and package they're on
      .groupBy("Id", "Project", "Level", "Severity")

      // Get earliest and latest date of vulnerability occurence in each package
      .agg(min("StartDate").as("StartDate"), max("EndDate").as("EndDate"))

      // Recalculate Duration
      .withColumn("Duration", floor(datediff(col("EndDate"), col("StartDate")) / 30))

      // Get censor status (1 for observed, 0 for censored)
      .withColumn("Uncensored", when(col("EndDate") < Timestamp.from(CENSOR_DATE), 1).otherwise(0))

      // Reorder columns
      .select("Id", "Project", "Level", "Severity", "StartDate", "EndDate", "Duration", "Uncensored")
  }
}
