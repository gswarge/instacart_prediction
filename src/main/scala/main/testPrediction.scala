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

    def generateSimilarItems(testDf:DataFrame, similarityDf:DataFrame ) = {

        println("\nSimilarities Dataframe: ")
        similarityDf.show(10)
        println("\nInput Test Dataframe Length: "+testDf.count())
        testDf.show(10)
        val singleSample = testDf.limit(1)       

        val sampleSimilarities = similarityDf.alias("similarityDf")
                            .join(singleSample.alias("singleSample"),
                            col("similarityDf.product_id_right") === col("singleSample.product_id"),"inner")
                            .drop(col("singleSample.product_id"))
                            .sort($"cosine_similarity".desc)
           
        println("\nTotal Similarities found based on correlation: "+sampleSimilarities.count())
        sampleSimilarities.show(25, false)

    }
}