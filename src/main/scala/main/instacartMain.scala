package main

import etl.objDataProcessing
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
    //objDataProcessing.loadandProcessData()
    ItemMatrixFunc.loadProcessedData()
    
    objDataProcessing.spark.stop()
  }
}


