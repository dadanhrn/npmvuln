import java.io.FileInputStream

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.{Edge, VertexId}
import java.util.Properties

import npmvuln.jobs._
import npmvuln.props._
import org.apache.spark.sql.hive.HiveContext

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
    val spark: HiveContext = new HiveContext(sc)

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

    // Build dependencies dataframe from libio-dependencies.csv
    val libioDependenciesPath: String = properties.getProperty("data.dependencies")
    val dependenciesDf: DataFrame = DependenciesDfBuilder.build(spark, libioDependenciesPath)

    /***********************************
    * Run recursive vulnerability scan *
    ***********************************/
    val maxLevel: Int = properties.getProperty("pregel.maxIterations").toInt
    val scannedDf: DataFrame = VulnerabilityScan2.run(releasesDf, dependenciesDf, advisoryDf, maxLevel)

    if (properties.getProperty("save.scanned", "false") == "true") {
      val scannedSavePath: String = properties.getProperty("save.scanned.path")
      Persistence.saveDfAsCsv(scannedDf, scannedSavePath)
    }

    /*****************
    * Tidy up result *
    *****************/
    val resultDf: DataFrame = ResultDfBuilder1.build(scannedDf)

    if (properties.getProperty("save.result") == "true") {
      val resultSavePath: String = properties.getProperty("save.result.path")
      Persistence.saveDfAsCsv(resultDf, resultSavePath)
    }

  }

}
