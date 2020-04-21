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
    Run first 3 steps only once, it writes out a processed parquet file to be used for createItemMatrixDF()
*/
    //val processedDf = objDataProcessing.ingestAndProcessData()
    //val filteredDf = objDataProcessing.generateDepWiseSample(processedDf)
    //objDataProcessing.writeToParquet(filteredDf,"data/filteredf.parquet")
    //Step 4 and beyond
    objItemMatrix.loadProcessedData()
    objDataProcessing.spark.stop()
  }
}


