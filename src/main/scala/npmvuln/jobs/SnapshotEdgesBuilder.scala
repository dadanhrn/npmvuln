package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{Edge, VertexId}
import npmvuln.props.SnapshotEdge

import scala.reflect.ClassTag

object SnapshotEdgesBuilder {

  def build(projectsDf: DataFrame, releasesDf: DataFrame): RDD[Edge[SnapshotEdge]] = {

    // Join projects dataframe and releases dataframe
    projectsDf
      .join(releasesDf, projectsDf("Project") === releasesDf("Project"))

      // Build edges and properties
      .map(row => {
        val packageId: VertexId = row.getAs[VertexId]("ProjectId")
        val packageStateId: VertexId = row.getAs[VertexId]("ReleaseId")
        val packageName: String = row.getAs[String]("Project")
        val version: String = row.getAs[String]("Release")
        val snapshotProp: SnapshotEdge = new SnapshotEdge(packageName, version)

        new Edge[SnapshotEdge](packageStateId, packageId, snapshotProp)
      }) (ClassTag(classOf[Edge[SnapshotEdge]]))

  }
}
