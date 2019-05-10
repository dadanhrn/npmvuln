import java.io.FileInputStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeRDD, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import java.util.Properties

import npmvuln.jobs._
import npmvuln.props._
import org.apache.spark.storage.StorageLevel

object Main extends App {
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
      classOf[(VertexId, VulnProperties)],
      classOf[Edge[Null]]
    ))

  val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate

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
    .build(releasesDf, advisoryDf).cache

  // Build PackageState vertices RDD
  val packageStateVertices: RDD[(VertexId, PackageStateVertex)] = PackageStateVerticesBuilder
    .build(releasesDf, vulnProperties).cache

  // Build DEPENDS_ON edges RDD
  val dependsOnEdges: RDD[Edge[Null]] = DependsOnEdgesBuilder
    .build(dependenciesDf, releasesDf).cache

  /**************
  * Build graph *
  **************/
  // Build graph
  val graph: Graph[PackageStateVertex, Null] = Graph(packageStateVertices, dependsOnEdges)
    .cache

  /*****************
  * Execute Pregel *
  *****************/
  // Execute Pregel program
  val maxIterations: Int = properties.getProperty("pregel.maxIterations").toInt
  val result: Graph[PackageStateVertex, Null] = VulnerabilityScan.run(graph, maxIterations)
    .cache

  // Build propagated vulnerabilities dataframe
  val resultDf: DataFrame = ResultDfBuilder.run(spark, result)

  // Save graph
  if (properties.getProperty("save.graph") == "true"){
    val vertexSavePath: String = properties.getProperty("save.vertex.path")
    val edgeSavePath: String = properties.getProperty("save.edge.path")
    Persistence.saveGraph(result, vertexSavePath, edgeSavePath)
  }

  // Save propagated vulnerabilities dataframe
  if (properties.getProperty("save.result") == "true"){
    val resultSavePath: String = properties.getProperty("save.result.path")
    Persistence.saveDfAsCsv(resultDf, resultSavePath)
  }
}