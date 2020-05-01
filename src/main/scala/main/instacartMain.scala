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
    val noOfTestSamples = 5
    val saveItemSimMatPath = "data/itemSimilarityMatrix.txt"
    val similarityDfCsvPath = "data/productSimilarities.csv"
    val similarityDfParquetPath = "data/productSimilarities.parquet"
  
/*
    Run first 2 steps only once, it writes out a processed parquet file to be used for createItemMatrixDF()
*/  
    //========================================================================
    // Step 1: Load and merge csv files
    //val processedDf = objDataProcessing.ingestAndProcessData(fullProcessedDfPath)

    //========================================================================
    //step 2: Generate a subset of the sample for dev purpose and save subset for dev purposes
    
    //val filteredDf = objDataProcessing.generateDepWiseSample(processedDf)
    //objDataProcessing.writeToParquet(filteredDf,filteredDfPath)

    //========================================================================
    //Step 3 : Load processed data, by default it'll load the subset of the full dataset, dataset of department alcohol
    //use args[0] for commandline paths
    //val processedDf = objDataProcessing.getParquet(filteredDfPath)
    val processedDf = objDataProcessing.getParquet(fullProcessedDfPath)
  
    //========================================================================
    //step 4: Generate ItemItemMatrix
    //val itemMatrixDf = objItemMatrix.generateItemItemMatrix(processedDf)
    //or
    //val itemMatrixDf = objDataProcessing.readCSV("data/ItemItemMatrix.csv")
    
    //========================================================================
    //Step 5: Generate UserItemMatrix
    //val userItemMatrixDf = objItemMatrix.generateUserItemMatrix(processedDf)
    //objDataProcessing.writeToParquet(itemMatrixDf,"data/fullUserItemMatrix.parquet")

    //Using Spark's ALS algorithm
    //val userItemMatridDf = objItemMatrix.userItemMatrixAls(processedDf)

    //========================================================================
    //Step 6: Normalise generated Matrix
    //val normalisedItemMatrix = objItemMatrix.generateNormalisedMatrix(itemMatrixDf)
    //val normalisedUserItemMatrix = objItemMatrix.generateNormalisedMatrix(userItemMatridDf)
    
    //========================================================================
    //Step 7: Generate Similarties BETWEEN PRODUCTS using Cosine Similarities
    //Similarities based on correlation of purchases.
    //SIMILARITIES GENERATED, TOOK 15MINS, total 47 Mins to generate similarities and save as CSV

    //val similarityDf = objCosineSimilarity.generateCosineSimilartyWithoutMatrix(processedDf,saveItemSimMatPath)

    //Reading already generated similarity ratings
    val similarityDf = objDataProcessing.readCSV(similarityDfCsvPath)
    
    //========================================================================
    //step 8: test predictions using generated similarities
    val testItems = processedDf.sample(true, 0.1).limit(noOfTestSamples).toDF()
    objTestPredictions.predictSimilarItems(testItems.select("product_id","product_name"),similarityDf)
    
    //========================================================================
    //step n: Stop spark session before finishing
    objDataProcessing.spark.stop()



  }
}


