package npmvuln.props

case class DependsOnEdge(var dependencyConstraint: String)
extends EdgeProperties with Serializable