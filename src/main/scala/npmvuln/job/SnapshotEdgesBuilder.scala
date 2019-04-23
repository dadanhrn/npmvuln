package npmvuln.job

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{VertexId, Edge}
import org.threeten.extra.Interval
import java.sql.Timestamp
import java.time.Instant
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
        val from: Instant = row.getAs[Timestamp]("Date").toInstant
        val to: Instant = row.getAs[Timestamp]("NextReleaseDate").toInstant
        val period: Interval = Interval.of(from, to)
        val snapshotProp: SnapshotEdge = new SnapshotEdge(packageName, version, period)

        new Edge[SnapshotEdge](packageStateId, packageId, snapshotProp)
      }) (Encoders.kryo(classOf[Edge[SnapshotEdge]]))

      // Get RDD
      .rdd
  }
}
