package helpers

import org.apache.spark.sql.SparkSession

object SparkWrapper {

  val args = Args.getArgs()
  var builder:SparkSession.Builder = null
  var spark:SparkSession = null
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

  def getSession():SparkSession = {
    createSessionIfNotExists()
    spark
  }
}
