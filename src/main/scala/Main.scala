import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeRDD, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import npmvuln.job._
import npmvuln.props._

object Main extends App {

  /**************************
  * Build execution context *
  **************************/
  val conf: SparkConf = new SparkConf()
    .setMaster("spark://spark-master:7077")
    .setAppName("NPMVuln")
  val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession.builder.master("spark://spark-master:7077").getOrCreate

  /*******************
  * Build dataframes *
  *******************/
  // Build advisory dataframe
  val advisoryPath: String = "file:///home/cerdas/Documents/dadanhrn/replicationpackage/data/vulnerabilities.csv"
  val advisoryDf: DataFrame = AdvisoryDfBuilder.build(spark, advisoryPath).cache()

  // Build release dataframe from libio-versions.csv
  val libioVersionsPath: String = "file:///home/cerdas/Documents/dadanhrn/replicationpackage/data/libio-versions1.csv"
  val releasesDf: DataFrame = ReleaseDfBuilder.build(spark, libioVersionsPath, advisoryDf).cache()

  // Build project dataframe from release dataframe
  val projectsDf: DataFrame = ProjectDfBuilder.build(releasesDf).cache()

  // Build dependencies dataframe from libio-dependencies.csv
  val libioDependenciesPath: String = "file:///home/cerdas/Documents/dadanhrn/replicationpackage/data/libio-dependencies1.csv"
  val dependenciesDf: DataFrame = DependenciesDfBuilder.build(spark, libioDependenciesPath).cache()

  releasesDf.filter(releasesDf("Project") === "marked").show()

  /***************************
  * Build vertices and edges *
  ***************************/
  // Build Package vertices RDD
  val packageVertices: RDD[(VertexId, PackageVertex)] = PackageVerticesBuilder
    .build(projectsDf)

  // Build PackageState vertices RDD
  val packageStateVertices: RDD[(VertexId, PackageStateVertex)] = PackageStateVerticesBuilder
    .build(releasesDf)

  // Build SNAPSHOT edges RDD
  val snapshotEdges: RDD[Edge[SnapshotEdge]] = SnapshotEdgesBuilder
    .build(projectsDf, releasesDf)

  // Build DEPENDS_ON edges RDD
  val dependsOnEdges: RDD[Edge[DependsOnEdge]] = DependsOnEdgesBuilder
    .build(dependenciesDf, projectsDf, releasesDf)

  /**************
  * Build graph *
  **************/
  // Build vertex RDD by merging Package and PackageState RDDs
  val vertexRDD: RDD[(VertexId, VertexProperties)] = sc
    .union(Seq(packageVertices.asInstanceOf[RDD[(VertexId, VertexProperties)]], packageStateVertices.asInstanceOf[RDD[(VertexId, VertexProperties)]]))

  // Build edge RDD by merging SNAPSHOT and DEPENDS_ON RDDs
  val edgeRDD: RDD[Edge[EdgeProperties]] = sc
    .union(Seq(snapshotEdges.asInstanceOf[RDD[Edge[EdgeProperties]]], dependsOnEdges.asInstanceOf[RDD[Edge[EdgeProperties]]]))

  // Build graph
  val graph: Graph[VertexProperties, EdgeProperties] = Graph(vertexRDD, edgeRDD).cache()

}