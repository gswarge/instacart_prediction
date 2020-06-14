package main

import etl.objDataProcessing
import org.apache.log4j.Logger
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.linalg.distributed._

class instacartMain extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
}

object instacartMain extends Serializable{

  val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings
    val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    println("Initialising main...")
    val filteredDfPath = "data/filteredf.parquet"
    val fullProcessedDfPath = "data/fullOrderProducts.parquet"
    val noOfTestSamples = 50
    val allPriorOrdersCsvPath = "data/processed/allPriorOrderProductsUsers.csv"
    val saveItemSimMatPath = "data/processed/priorOrdersProdSimilarityMatrix.txt"
    val similarityDfPath = "data/processed/concatfiles/allPriorOrdersProductsSims.txt"
    
  
/*
    Run first 2 steps only once, it writes out a processed parquet file to be used for next step
*/
    //========================================================================
    // Step 1: Load, merge & split csv files
    //objDataProcessing.ingestAndProcessData(fullProcessedDfPath)
    
    
    //========================================================================
    //Step : Generate Similarties BETWEEN PRODUCTS using Cosine Similarities
    //Similarities based on correlation of purchases.
    //SIMILARITIES GENERATED,took, total 47 Mins to generate similarities and save as CSV
    //val priorOrdersOfUsersDf = objDataProcessing.readCSV(allPriorOrdersOfUsersCsvPath)
    //val similarityMat = objCosineSimilarity.generateCosineSimilarties(priorOrdersOfUsersDf,saveItemSimMatPath)

    //Reading already generated product similarity matrix from prior orders
    //val similarityMat = objDataProcessing.readCSV(similarityDfPath)


    //========================================================================
    //step : generate Similar Items, using Cosine similarities
    //objTestPredictions.generateSimilarItems(testItemsDf,similarityMat,processedDf,"cosine")
    objGeneratePredictions.extractLast2Orders(similarityDfPath,allPriorOrdersCsvPath)

    
    //========================================================================
    //generate Cooccurances
    //val (cooccuranceDf,cooccuranceMat) = objItemMatrix.generateCooccurances(processedDf,filteredDfCooccurances)
    //Read generated Cooccurances Matrix
    //val cooccuranceMat = objDataProcessing.readCSV(fullDfCooccurances)

    //========================================================================
    //Decompose the cooccurance matrix using SVD 
    //objModels.applySVD(cooccuranceMat,testItemsDf)
    
    //========================================================================
    //Step: train ALS algorithm on cooccurance Matrix
    //objModels.applyItemItemALS(cooccuranceDf,testItemsDf)

    //Step: generate similar items using cooccurance matrix
    //objTestPredictions.generateSimilarItems(testItemsDf,cooccuranceMat,processedDf,"cooccurance")

    //========================================================================
    //step n: Stop spark session before finishing
    objDataProcessing.spark.stop()



  }
}


