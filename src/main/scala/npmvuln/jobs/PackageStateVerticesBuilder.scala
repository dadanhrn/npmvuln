package npmvuln.jobs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.graphx.VertexId
import java.sql.Timestamp

import npmvuln.props._

object PackageStateVerticesBuilder {

  def build(releasesDf: DataFrame, vulnProperties: RDD[(VertexId, Array[VulnProperties])]):
  RDD[(VertexId, PackageStateVertex)] = {

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
      }) (Encoders.kryo(classOf[(VertexId, PackageStateVertex)]))


      // Get RDD
      .rdd

      // Join with vulnerabilities RDD
      .leftOuterJoin(vulnProperties)

      // Attach list of vulnerabilities into PackageState vertex
      .mapValues(pair => {
        // Attach if list present. If not, give empty array.
        pair._2 match {
          case Some(lsVuln) => {
            // Add PackageStateVertex it's on to propagation path
            pair._1.vulnRecords = lsVuln.map(vulnProp => {
              vulnProp.immediateSource = pair._1
              vulnProp
            })
          }

          case None => pair._1.vulnRecords = Array()
        }

        // Return PackageState vertex properties
        pair._1
      })
  }

}
