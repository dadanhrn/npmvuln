package npmvuln.props

import org.apache.spark.graphx.{Edge, VertexId}
import scala.util.hashing.MurmurHash3.stringHash

case class DependsOnEdge(var dependencyConstraint: String)
extends Serializable