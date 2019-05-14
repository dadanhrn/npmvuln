package npmvuln.props

import java.sql.Timestamp
import org.threeten.extra.Interval

case class SnapshotEdge(var packageName: String, var version: String)
extends EdgeProperties with Serializable