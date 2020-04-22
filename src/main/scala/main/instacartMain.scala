package main

import etl.objDataProcessing
import org.apache.log4j.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel}

class instacartMain extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacartMain extends Serializable{

  def main(args: Array[String]): Unit = {

    println("Initialising main...")
    val filteredDfPath = "data/filteredf.parquet"
    val fullProcessedDfPath = "data/fullOrderProductsDf.parquet"
  
/*
    Run first 2 steps only once, it writes out a processed parquet file to be used for createItemMatrixDF()
*/
    // Step 1: Load and merge csv files
    //val processedDf = objDataProcessing.ingestAndProcessData(fullProcessedDfPath)
    
    //step 2: Generate a subset of the sample for dev purpose and save subset for dev purposes
    
    //val filteredDf = objDataProcessing.generateDepWiseSample(processedDf)
    //objDataProcessing.writeToParquet(filteredDf,filteredDfPath)


    //Step 3 : Load processed data, by default it'll load the subset of the full dataset
    //use args[0] for commandline paths
    val processedDf = objDataProcessing.getParquet(fullProcessedDfPath)
    
    //step 4: Generate ItemItemMatrix
    val itemMatrixDf = objItemMatrix.generateItemItemMatrix(processedDf)
    //objDataProcessing.writeToCSV(itemMatrixDf,"data/ItemItemMatrix.csv")

    //Step 5: Normalise generated Matrix
    //val itemMatrixDf = objDataProcessing.readCSV("data/ItemItemMatrix.csv")
    val normalisedMatrix = objItemMatrix.generateNormalisedMatrix(itemMatrixDf)
    

    //step n: Stop spark session before finishing
    objDataProcessing.spark.stop()



  }
}


