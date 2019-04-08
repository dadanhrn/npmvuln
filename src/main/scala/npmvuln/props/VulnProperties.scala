package npmvuln.props

import java.sql.Timestamp

class VulnProperties(var from: Timestamp, var to: Timestamp, var level: Int, var severity: String)
