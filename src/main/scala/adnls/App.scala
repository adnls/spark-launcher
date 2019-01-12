package adnls

import java.io.{BufferedReader, PrintWriter, StringReader, StringWriter}
import java.net.URLClassLoader

import helpers.Args
import org.apache.spark.{SparkConf, repl}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.GenericRunnerSettings

/**
 * @author ${user.name}
 */

object App {

  def runInterpreter(input: String, conf: SparkConf) = {

    val in = new BufferedReader(new StringReader(input + "\n"))
    val out = new StringWriter()
    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]

    cl match {
      case urlLoader: URLClassLoader =>
        for (url <- urlLoader.getURLs) {
          if (url.getProtocol == "file") {
            paths += url.getFile
          }
        }
      case _ =>
    }

    repl.Main.conf.setAll(conf.getAll)
    val interp = new repl.SparkILoop(Some(in), new PrintWriter(out))
    repl.Main.interp = interp
    val separator = System.getProperty("path.separator")
    val settings = new GenericRunnerSettings(s => throw new RuntimeException(s"Scala options error: $s"))
    settings.processArguments(List("-classpath", paths.mkString(separator)), true)
    interp.process(settings) // Repl starts and goes in loop of R.E.P.L
    repl.Main.interp = null
    Option(repl.Main.sparkContext).foreach(_.stop())
    System.clearProperty("spark.driver.port")
    out.toString
  }

  def main(args: Array[String]) {

    Args.parse(args)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkLauncher")

    val command =
      """
        val myDF = spark.read.option("header", "true").option("sep", ",").csv("test.csv")
        myDF.show()

      """.stripMargin

    val res = runInterpreter(command, conf)
    println(res)
  }
}
