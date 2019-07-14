package npmvuln.jobs

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, max, min, udf}
import java.time.Duration

import npmvuln.helpers.constants.CENSOR_DATE

object ResultDfBuilder1 {
  val getDuration: UserDefinedFunction = udf[Long, Timestamp, Timestamp]((start_date, end_date) =>{
    Duration.between(start_date.toInstant, end_date.toInstant).toDays
  })

  def build(scannedDf: DataFrame): DataFrame = {
    scannedDf
      .groupBy("Id", "Project", "Release", "Level")
      .agg(min("StartDate").as("StartDate"), max("EndDate").as("EndDate"))
      .withColumn("Duration", getDuration(col("StartDate"), col("EndDate")))
      .withColumn("Uncensored", col("EndDate") =!= Timestamp.from(CENSOR_DATE))
      .withColumnRenamed("StartDate", "Since")
      .withColumnRenamed("EndDate", "To")
      .select("Id", "Name", "Severity", "Package", "Release",
        "Since", "To", "Duration", "Uncensored", "Level")
  }
}
