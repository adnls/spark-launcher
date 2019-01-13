package adnls

import java.io._
import helpers._

/**
 * @author ${user.name}
 */

object App {

  def main(args: Array[String]) {

    Args.parse(args)

    //val oldPs = System.out
    //val fOut = new ByteArrayOutputStream()
    //val psOut = new PrintStream(fOut)
    //System.setOut(psOut)
    //System.setOut(oldPs)

    val fErr = new File("log/log.txt")
    val psErr = new PrintStream(fErr)
    System.setErr(psErr)

    val commands =
      """
        val myDF = spark.read.option("header", "true").option("sep", ",").csv("data/in/test.csv")
        myDF.show()
        myDF.count()
        myDF.printSchema()
        myDF.withColumn("bool", lit("true")).write.mode("overwrite").option("header", "true").option("sep", ",").csv("data/out")

      """.stripMargin

    val result = SparkInterpreter.run(commands)

    System.out.println(result)
  }
}
