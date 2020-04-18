package main

import etl.DataProcessing
import org.apache.log4j.Logger

class instacartMain extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacartMain extends Serializable{

  def main(args: Array[String]): Unit = {

    println("Initialising main...")
  
/*
    Run loadandProcessData() only once, it writes out a processed parquet file to be used for createItemMatrixDF()
*/
    //DataProcessing.loadandProcessData()
    ItemMatrixFunc.loadProcessedData()
    
    DataProcessing.spark.stop()
  }
}


