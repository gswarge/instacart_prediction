package main

import etl.DataProcessing
import org.apache.log4j.Logger

class instacart_main extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacart_main extends Serializable{

  def main(args: Array[String]): Unit = {

    println("testing main")
    DataProcessing.loadData()
    val dummy = args(0)
    /*
    dummy match {
      case "GrabData" => DataProcessing.getParquet(args(1))
      case "PrintCSV" => DataProcessing.readCSV(args(1))
      case "loadData" => DataProcessing.loadData(args(1))
      case _ => throw new ClassNotFoundException(s"$dummy class does not exist !")
    }
    */
  }
}


