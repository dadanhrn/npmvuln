package npmvuln.jobs

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import npmvuln.props._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Persistence {

  def saveGraph(graph: Graph[PackageStateVertex, Null], vertexPath: String, edgePath: String): Unit = {
    graph.vertices.saveAsObjectFile(vertexPath)
    graph.edges.saveAsObjectFile(edgePath)
  }

  def loadGraph(sc: SparkContext, vertexPath: String, edgePath: String): Graph[PackageStateVertex, Null] = {
    val vertexRDD: RDD[(VertexId, PackageStateVertex)] = sc.objectFile[(VertexId, PackageStateVertex)](vertexPath)
    val edgeRDD: RDD[Edge[Null]] = sc.objectFile[Edge[Null]](edgePath)
    Graph[PackageStateVertex, Null](vertexRDD, edgeRDD)
  }

  def saveDfAsCsv(df: DataFrame, path: String): Unit = {
    df.rdd.saveAsTextFile(path)
  }

}
