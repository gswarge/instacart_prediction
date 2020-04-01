package main

import etl.DataProcessing
import org.apache.log4j.Logger

class instacart_main extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacart_main extends Serializable{

  def main(args: Array[String]): Unit = {

    println("testing main")
    var dir = "data/"
    val dataFiles = DataProcessing.getListOfFiles(dir)
    println("Files:\n" + dataFiles)
    DataProcessing.loadData()
    DataProcessing.spark.stop()
  }
}


