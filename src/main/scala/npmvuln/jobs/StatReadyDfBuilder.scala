package npmvuln.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, floor, lit, max, min, udf}
import org.threeten.extra.Interval
import java.sql.Timestamp
import npmvuln.helpers.constants.CENSOR_DATE

object StatReadyDfBuilder {
  val getDurationInDay: UserDefinedFunction = udf[Int, Timestamp, Timestamp]((since, to) => {
    Interval.of(since.toInstant, to.toInstant).toDuration.toDays.toInt
  })

  val getCensorStatus: UserDefinedFunction = udf[Int, Timestamp](endTime => {
    if (endTime.toInstant == CENSOR_DATE) 0 else 1
  })

  def build(resultDf: DataFrame): DataFrame = {

    // Source dataframe
    resultDf

      // Group aggregation by vulnerability ID and package they're on
      .groupBy("Id", "Package")

      // Get earliest and latest date of vulnerability occurence in each package
      .agg(min("Since").as("Since"), max("To").as("To"))

      // Calculate difference between latest and earliest occurence
      .withColumn("Duration", floor(getDurationInDay(col("Since"), col("To")) / 30))

      // Get censor status (1 for observed, 0 for censored)
      .withColumn("Uncensored", getCensorStatus(col("To")))
  }
}
