package npmvuln.jobs

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import npmvuln.props._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Persistence {

  def saveGraph(graph: Graph[VertexProperties, EdgeProperties], vertexPath: String, edgePath: String): Unit = {
    graph.vertices.saveAsObjectFile(vertexPath)
    graph.edges.saveAsObjectFile(edgePath)
  }

  def loadGraph(sc: SparkContext, vertexPath: String, edgePath: String): Graph[VertexProperties, EdgeProperties] = {
    val vertexRDD: RDD[(VertexId, VertexProperties)] = sc.objectFile[(VertexId, VertexProperties)](vertexPath)
    val edgeRDD: RDD[Edge[EdgeProperties]] = sc.objectFile[Edge[EdgeProperties]](edgePath)
    Graph[VertexProperties, EdgeProperties](vertexRDD, edgeRDD)
  }

  def saveDfAsCsv(df: DataFrame, path: String): Unit = {
    df.write.csv(path)
  }

}
