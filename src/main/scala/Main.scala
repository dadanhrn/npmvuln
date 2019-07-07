import java.io.FileInputStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import java.util.Properties

import npmvuln.jobs._
import npmvuln.props._

object Main extends App {

  override def main(args: Array[String]) {

    /******************
    * Read properties *
    ******************/
    val propertiesFile: FileInputStream = new FileInputStream("./npmvuln.properties")
    val properties: Properties = new Properties()
    properties.load(propertiesFile)
    propertiesFile.close()

    /**************************
    * Build execution context *
    **************************/
    val conf: SparkConf = new SparkConf()
      .setAppName("NPMVuln")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(
        classOf[(VertexId, PackageStateVertex)],
        classOf[(VertexId, PackageVertex)],
        classOf[(VertexId, VulnProperties)],
        classOf[Edge[SnapshotEdge]],
        classOf[Edge[DependsOnEdge]]
      ))

    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

    val checkpointDir: String = properties.getProperty("sc.checkpointDir")
    sc.setCheckpointDir(checkpointDir)

    /*******************
    * Build dataframes *
    *******************/
    // Build advisory dataframe
    val advisoryPath: String = properties.getProperty("data.advisory")
    val advisoryDf: DataFrame = AdvisoryDfBuilder.build(spark, advisoryPath)

    // Build release dataframe from libio-versions.csv
    val libioVersionsPath: String = properties.getProperty("data.versions")
    val releasesDf: DataFrame = ReleaseDfBuilder.build(spark, libioVersionsPath)

    // Build project dataframe from release dataframe
    val projectsDf: DataFrame = ProjectDfBuilder.build(releasesDf)

    // Build dependencies dataframe from libio-dependencies.csv
    val libioDependenciesPath: String = properties.getProperty("data.dependencies")
    val dependenciesDf: DataFrame = DependenciesDfBuilder.build(spark, libioDependenciesPath)

    /***************************
    * Build vertices and edges *
    ***************************/
    // Get vulnerability properties
    val vulnProperties: RDD[(VertexId, Array[VulnProperties])] = VulnerabilityDfBuilder
      .build(releasesDf, advisoryDf)

    // Build Package vertices RDD
    val packageVertices: RDD[(VertexId, PackageVertex)] = PackageVerticesBuilder
      .build(projectsDf)

    // Build PackageState vertices RDD
    val packageStateVertices: RDD[(VertexId, PackageStateVertex)] = PackageStateVerticesBuilder
      .build(releasesDf, vulnProperties)

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
      .union(Seq(packageVertices.asInstanceOf[RDD[(VertexId, VertexProperties)]],
        packageStateVertices.asInstanceOf[RDD[(VertexId, VertexProperties)]]))
      .repartition(2500)
    vertexRDD.checkpoint()

    // Build edge RDD by merging SNAPSHOT and DEPENDS_ON RDDs
    val edgeRDD: RDD[Edge[EdgeProperties]] = sc
      .union(Seq(snapshotEdges.asInstanceOf[RDD[Edge[EdgeProperties]]],
        dependsOnEdges.asInstanceOf[RDD[Edge[EdgeProperties]]]))
      .repartition(2000)
    edgeRDD.checkpoint()

    // Build graph
    val graph: Graph[VertexProperties, EdgeProperties] = Graph(vertexRDD, edgeRDD)
      .partitionBy(PartitionStrategy.EdgePartition2D)

    /*****************
    * Execute Pregel *
    *****************/
    // Execute Pregel program
    var maxIterations: Int = properties.getProperty("pregel.maxIterations").toInt
    val result: Graph[VertexProperties, EdgeProperties] = VulnerabilityScan.run(graph, maxIterations)

    // Build propagated vulnerabilities dataframe
    val resultDf: DataFrame = ResultDfBuilder.run(spark, result)

    // Save graph
    if (properties.getProperty("save.graph") == "true") {
      val vertexSavePath: String = properties.getProperty("save.vertex.path")
      val edgeSavePath: String = properties.getProperty("save.edge.path")
      Persistence.saveGraph(result, vertexSavePath, edgeSavePath)
    }

    // Save propagated vulnerabilities dataframe
    if (properties.getProperty("save.result") == "true") {
      val resultSavePath: String = properties.getProperty("save.result.path")
      Persistence.saveDfAsCsv(resultDf, resultSavePath)
    }

    val affectedpkg: Long = result.vertices
      .map(_._2)
      .filter(_.isInstanceOf[PackageVertex])
      .map(_.asInstanceOf[PackageVertex])
      .filter(_.vulnerabilities.length > 0)
      .count
    println("Affected package: " + affectedpkg.toString)

  }

}
