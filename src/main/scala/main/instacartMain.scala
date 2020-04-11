package main

import etl.DataProcessing
import org.apache.log4j.Logger

class instacartMain extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacartMain extends Serializable{

  def main(args: Array[String]): Unit = {

    println("Initialising main...")
    //var dir = "data/"
    //val dataFiles = DataProcessing.getListOfFiles(dir)
    // Loading Data, Merging Data and Filtering it to a smaller dataframe for ItemMatrix Generation and writes to parquet, Run once and then run ItemMatrix
    //DataProcessing.processData()
    itemMatrixFunc.createItemMatrixDF()
    DataProcessing.spark.stop()
  }
}


