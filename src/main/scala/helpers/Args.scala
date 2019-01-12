package helpers

import org.rogach.scallop._

class Parser (args:Seq[String]) extends ScallopConf(args) {
  val env = opt[String](name = "env", default = Some("local"), short = 'e')
  val paths = props[String]('P')
  val write = toggle(name="write", default = Some(true), prefix = "no-", short = 'w')
  verify()
}

object Args {
  private var conf:Parser = null
  def parse(args:Seq[String]) = conf = new Parser(args)
  def getArgs():Parser = return conf
}
