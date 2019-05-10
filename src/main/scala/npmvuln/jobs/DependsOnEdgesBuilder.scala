package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, Edge}
import npmvuln.jobs.VulnerabilityDfBuilder.checkConstraint

object DependsOnEdgesBuilder {

  def build(dependenciesDf: DataFrame, releasesDf: DataFrame): RDD[Edge[Null]] = {

    // Get second release dataframe
    val releasesDfRight: DataFrame = releasesDf.as("ReleasesDfRight")

    // Source dataframe
    releasesDf

      // Join with dependencies dataframe
      .join(dependenciesDf,
        releasesDf("Project") === dependenciesDf("Project") && releasesDf("Release") === dependenciesDf("Release"))

      // Join with itself by matching constraint
      .join(releasesDfRight,
        releasesDfRight("Project") === dependenciesDf("Dependency") &&
          checkConstraint(releasesDfRight("Release"), dependenciesDf("Constraint")))

      // Select column to be used
      .select(releasesDf("ReleaseId").as("DependentId"), releasesDfRight("ReleaseId").as("DependencyId"))

      // Build edges
      .map(row => {
        val dependentId: VertexId = row.getAs[VertexId]("DependentId")
        val dependencyId: VertexId = row.getAs[VertexId]("DependencyId")

        new Edge[Null](dependencyId, dependentId)
      }) (Encoders.kryo(classOf[Edge[Null]]))

      // Get RDD
      .rdd
  }
}
