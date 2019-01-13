package helpers

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkWrapper {

  val args = Args.getArgs()
  var builder:SparkSession.Builder = null
  var spark:SparkSession = null
  var conf:SparkConf = null
  val env = args.env()

  private def createSessionIfNotExists()= {
    if (spark == null) {
      builder = SparkSession
                  .builder
                  .appName("SparkLauncher")
      if (env == "local"){
        builder.master("local[*]")
      }
      spark = builder.getOrCreate()
    }
  }

  private def createConfIfNotExists() = {
    if (conf == null){
      conf = new SparkConf().setAppName("SparkLauncher")
      if (env == "local")
        conf.setMaster("local[*]")
    }
  }

  def getSession():SparkSession = {
    createSessionIfNotExists()
    spark
  }

  def getConf():SparkConf = {
    createConfIfNotExists()
    conf
  }
}
