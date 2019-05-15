package npmvuln.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, Edge}
import npmvuln.props.DependsOnEdge

object DependsOnEdgesBuilder {

  def build(dependenciesDf: DataFrame, projectsDf: DataFrame, releasesDf: DataFrame): RDD[Edge[DependsOnEdge]] = {

    // Join dependencies dataframe, projects dataframe, and releases datafrane
    dependenciesDf
      .join(projectsDf, dependenciesDf("Dependency") === projectsDf("Project"))
      .join(releasesDf, Seq("Project", "Release"))

      // Build edges and properties
      .map(row => {
        val dependentId: VertexId = row.getAs[VertexId]("ReleaseId")
        val dependencyId: VertexId = row.getAs[VertexId]("ProjectId")
        val constraint: String = row.getAs[String]("Constraint")
        val dependencyProp: DependsOnEdge = new DependsOnEdge(constraint)

        new Edge[DependsOnEdge](dependencyId, dependentId, dependencyProp)
      })
  }
}
