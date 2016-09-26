package de.dlh.smile.axdelivery

import de.dlh.smile.engine.commons.Contexts
import de.dlh.smile.axdelivery.Harness

object Main {

  lazy val sc = Contexts.sc
  lazy val sqlCtx = Contexts.sqlCtx

  def main(args: Array[String]) {
    Harness.execute(sc, sqlCtx)
  }
}