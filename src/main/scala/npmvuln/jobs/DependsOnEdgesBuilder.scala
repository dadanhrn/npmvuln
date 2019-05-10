package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.col
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, Edge}
import npmvuln.jobs.VulnerabilityDfBuilder.checkConstraint

object DependsOnEdgesBuilder {

  def build(dependenciesDf: DataFrame, releasesDf: DataFrame): RDD[Edge[Null]] = {

    // Source dataframe
    releasesDf
      .crossJoin(
        releasesDf
          .withColumnRenamed("Project", "Dep_Project")
          .withColumnRenamed("Release", "Dep_Release")
          .withColumnRenamed("Date", "Dep_Date")
          .withColumnRenamed("NextReleaseDate", "Dep_NextReleaseDate")
          .withColumnRenamed("ReleaseId", "Dep_ReleaseId")
      )
      .join(dependenciesDf, Seq("Project", "Release"))
      .where(checkConstraint(col("Dep_Release"), col("Constraint")))

      // Select column to be used
      .select(col("ReleaseId"), col("Dep_ReleaseId"))

      // Build edges
      .map(row => {
        val dependentId: VertexId = row.getAs[VertexId]("ReleaseId")
        val dependencyId: VertexId = row.getAs[VertexId]("Dep_ReleaseId")

        new Edge[Null](dependencyId, dependentId)
      }) (Encoders.kryo(classOf[Edge[Null]]))

      // Get RDD
      .rdd
  }
}
