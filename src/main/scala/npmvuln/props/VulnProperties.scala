package npmvuln.props

import org.threeten.extra.Interval

class VulnProperties(var id: String, var name: String, var severity: String, var period: Interval,
                     var propagationPath: Array[PackageStateVertex] = Array.empty)
extends Serializable