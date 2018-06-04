package com.kinetica.spark.util.table

case class KineticaException(message: String = "", cause: Option[Throwable] = None)
    extends Exception(message) {
  cause.foreach(initCause)
}