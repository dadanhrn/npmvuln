package npmvuln.props

import java.sql.Timestamp
import org.threeten.extra.Interval

case class PackageStateVertex(var packageName: String, var version: String,
                              var latestPeriod: Interval,
                              var vulnRecords: Array[VulnProperties] = Array.empty)
extends Serializable