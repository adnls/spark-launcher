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

    val fErr = new File("log.txt")
    val psErr = new PrintStream(fErr)
    System.setErr(psErr)

    val commands =
      """
        val myDF = spark.read.option("header", "true").option("sep", ",").csv("test.csv")
        myDF.show()
        myDF.count()
        myDF.printSchema()

      """.stripMargin

    val result = SparkInterpreter.run(commands)

    System.out.println(result)
  }
}
