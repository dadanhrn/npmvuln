package npmvuln.props

case class PackageVertex(var packageName: String, var vulnerabilities: Array[VulnProperties] = Array())
extends VertexProperties with Serializable
