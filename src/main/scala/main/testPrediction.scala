package main

import etl.objDataProcessing
import shapeless.Data
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import spire.algebra.CoordinateSpace
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix

object objTestPredictions {
    println("Test Predictions..")
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings

    def predictSimilarItems(testDf:DataFrame, similarityDf:DataFrame ) = {

        println("\nInput Test Dataframe Length: "+testDf.count())
        println(testDf.show(10))
        val singleSample = testDf.select("product_id").limit(1)       
        
        println(singleSample.show(5))
        
        val sampleSimilarities = similarityDf.filter(similarityDf("product_id_left") === singleSample("product_id"))

        println(sampleSimilarities.show(10))

    }
}