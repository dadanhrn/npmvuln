package npmvuln.job

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, Edge}
import java.sql.Timestamp
import npmvuln.props.SnapshotEdge

object SnapshotEdgesBuilder {

  def build(projectsDf: DataFrame, releasesDf: DataFrame): RDD[Edge[SnapshotEdge]] = {

    // Join projects dataframe and releases dataframe
    projectsDf
      .join(releasesDf, "Project")

      // Build edges and properties
      .map(row => {
        val packageId: VertexId = row.getAs[VertexId]("ProjectId")
        val packageStateId: VertexId = row.getAs[VertexId]("ReleaseId")
        val packageName: String = row.getAs[String]("Project")
        val version: String = row.getAs[String]("Release")
        val from: Timestamp = row.getAs[Timestamp]("Date")
        val to: Timestamp = row.getAs[Timestamp]("NextReleaseDate")
        val snapshotProp: SnapshotEdge = new SnapshotEdge(packageName, version, from, to)

        new Edge[SnapshotEdge](packageStateId, packageId, snapshotProp)
      }) (Encoders.bean(classOf[Edge[SnapshotEdge]]))

      // Get RDD
      .rdd
  }
}
