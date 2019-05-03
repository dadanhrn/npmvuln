package npmvuln.helpers

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object CumulativeProduct extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("X", DoubleType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(
      StructField("Count", LongType) ::
        StructField("Product", DoubleType) ::
        Nil
    )
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1
    buffer(1) = buffer.getDouble(1) * input.getDouble(0)
  }

  override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
    buffer(0) = buffer.getLong(0) + row.getLong(0)
    buffer(1) = buffer.getDouble(1) + row.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(1)
  }
}