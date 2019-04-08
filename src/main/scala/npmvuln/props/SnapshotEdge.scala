package npmvuln.props

import java.sql.Timestamp

case class SnapshotEdge(var packageName: String, var version: String, var from: Timestamp, var to: Timestamp)
extends EdgeProperties with Serializable