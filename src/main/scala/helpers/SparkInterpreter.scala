package helpers

import java.io.{BufferedReader, PrintWriter, StringReader, StringWriter}
import java.net.URLClassLoader

import org.apache.spark.{SparkConf, repl}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.GenericRunnerSettings

object SparkInterpreter {

  val conf = SparkWrapper.getConf()

  //returns interpreter console as string
  def run(input: String) = {

    repl.Main.conf.setAll(conf.getAll)
    repl.SparkILoop.run(input,
      new GenericRunnerSettings(s => throw new RuntimeException(s"Scala options error: $s")))
  }

  //more control on output
  //TODO:find a way for being more intercative => bind params, keep interpreter alive, keep session state, etc.
  def runAlt(input: String) = {

    val in = new BufferedReader(new StringReader(input))
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

    var interp = new repl.SparkILoop(Some(in), new PrintWriter(out))

    repl.Main.interp = interp

    val separator = System.getProperty("path.separator")
    val settings = new GenericRunnerSettings(s => throw new RuntimeException(s"Scala options error: $s"))
    settings.processArguments(List("-classpath", paths.mkString(separator)), true)

    repl.Main.interp.process(settings) // Repl starts and goes in loop of R.E.P.L

    repl.Main.interp = null

    Option(repl.Main.sparkContext).foreach(_.stop())
    System.clearProperty("spark.driver.port")
    out.toString
  }
}
