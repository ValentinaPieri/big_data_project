package utils

import org.apache.spark.sql.SparkSession

import java.io.InputStream

object Commons {



  object DeploymentMode extends Enumeration {
    type DeploymentMode = Value
    val local, remote = Value
  }

  import DeploymentMode._

  def initializeSparkContext(deploymentMode: String, spark: SparkSession): Unit = {
    if(deploymentMode == remote){
      val stream: InputStream = getClass.getResourceAsStream(Config.credentialsPath)
      val lines = scala.io.Source.fromInputStream( stream ).getLines.toList

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "bytebuffer")

      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", lines(0))
      spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", lines(1))
    }
  }

  def getDatasetPath(remotePath: String): String = {
    return "s3://" + Config.s3bucketName + "/" + remotePath
  }

}
