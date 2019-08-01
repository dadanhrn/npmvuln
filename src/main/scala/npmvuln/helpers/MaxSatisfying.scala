package npmvuln.helpers

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import com.github.gundy.semver4j.SemVer

import collection.JavaConverters._

object MaxSatisfying extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    StructField("Version", StringType) ::
    StructField("Constraint", StringType) ::
    Nil)

  override def bufferSchema: StructType = StructType(
    StructField("MaxVersion", StringType) ::
    Nil
  )

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = "0.0.0"
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = SemVer.maxSatisfying(
      Set(buffer.getString(0), input.getString(0)).asJava,
      input.getString(1)
    )
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = SemVer.maxSatisfying(
      Set(buffer1.getString(0), buffer2.getString(0)).asJava,
      "*"
    )
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }

}
