package npmvuln

import org.apache.spark.graphx.{Graph, VertexId, Edge}
import npmvuln.props._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GraphPersistence {

  def save(graph: Graph[VertexProperties, EdgeProperties], vertexPath: String, edgePath: String): Unit = {
    graph.vertices.saveAsObjectFile(vertexPath)
    graph.edges.saveAsObjectFile(edgePath)
  }

  def load(sc: SparkContext, vertexPath: String, edgePath: String): Graph[VertexProperties, EdgeProperties] = {
    val vertexRDD: RDD[(VertexId, VertexProperties)] = sc.objectFile[(VertexId, VertexProperties)](vertexPath)
    val edgeRDD: RDD[Edge[EdgeProperties]] = sc.objectFile[Edge[EdgeProperties]](edgePath)
    Graph[VertexProperties, EdgeProperties](vertexRDD, edgeRDD)
  }

}
