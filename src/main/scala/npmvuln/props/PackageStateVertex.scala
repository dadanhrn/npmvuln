package npmvuln.props

import java.sql.Timestamp

case class PackageStateVertex(var packageName: String, var version: String,
                         var releaseDate: Timestamp, var vulnRecords: Array[VulnProperties] = Array())
extends VertexProperties with Serializable