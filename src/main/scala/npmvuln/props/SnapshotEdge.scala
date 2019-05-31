package npmvuln.props

case class SnapshotEdge(var packageName: String, var version: String)
extends EdgeProperties with Serializable