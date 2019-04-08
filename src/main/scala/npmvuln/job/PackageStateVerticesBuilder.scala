package npmvuln.job

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.graphx.VertexId
import java.sql.Timestamp
import npmvuln.props.PackageStateVertex

object PackageStateVerticesBuilder {

  def build(releasesDf: DataFrame): RDD[(VertexId, PackageStateVertex)] = {

    // Source dataset
    releasesDf

      // Build id and PackageStateVertex pair
      .map(row => {
        val releaseId: VertexId = row.getAs[VertexId]("ReleaseId")
        val packageName: String = row.getAs[String]("Project")
        val version: String = row.getAs[String]("Release")
        val releaseDate: Timestamp = row.getAs[Timestamp]("Date")
        val packageStateVertex: PackageStateVertex = new PackageStateVertex(packageName, version, releaseDate)

        (releaseId, packageStateVertex)
      }) (Encoders.bean(classOf[(VertexId, PackageStateVertex)]))


      // Get RDD
      .rdd
  }

}
