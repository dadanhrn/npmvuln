package npmvuln.jobs

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import npmvuln.props.PackageVertex

import scala.reflect.ClassTag

object PackageVerticesBuilder {

  def build(projectDf: DataFrame): RDD[(VertexId, PackageVertex)] = {

    // Source dataset
    projectDf

      // Build id and PackageVertex pair
      .map(row => {
        val projectId: VertexId = row.getAs[VertexId]("ProjectId")
        val projectName: String = row.getAs[String]("Project")
        val packageVertex: PackageVertex = new PackageVertex(projectName)

        (projectId, packageVertex)
      }) (ClassTag(classOf[(VertexId, PackageVertex)]))

  }

}
