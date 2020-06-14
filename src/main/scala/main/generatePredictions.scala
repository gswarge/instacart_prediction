package main

import etl.objDataProcessing
import shapeless.Data
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import spire.algebra.CoordinateSpace
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import spire.std.int
import org.apache.spark.sql.types.IntegerType

object objGeneratePredictions {

    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings


    def extractLast2Orders(similarityDfPath:String, allPriorOrdersCsvPath:String) = {
        
        
        println("\nReading allpriorOrders....")
        val allPriorOrdersDf = objDataProcessing.readCSV(allPriorOrdersCsvPath)
        // Compute the max age and average salary, grouped by department and gender.
        //ds.groupBy($"department", $"gender").agg(Map(
         //               "salary" -> "avg",
         //               "age" -> "max"
         //                          ))
        val last2UserOrderNo = allPriorOrdersDf
                            .groupBy("user_id")
                            .agg(max("order_number").as("lastOrder"))
                            .withColumn("2ndLastOrder",($"lastOrder"-1))
                            .sort($"user_id".desc)
                            
        last2UserOrderNo.show(25)
        val last2UserOrders = allPriorOrdersDf.alias("po")
                                .select("po.user_id","po.order_id","po.product_id","po.order_number")
                                .join(last2UserOrderNo.alias("2nd"),
                                ($"po.user_id" === $"2nd.user_id" && $"po.order_number" === $"2nd.lastOrder")).drop($"2nd.user_id",$"po.order_number")
        
        last2UserOrders.show(25)
        println("\nWriting last2UserOrders....\n")
        objDataProcessing.writeToCSV(last2UserOrders,"data/processed/last2UserOrders.csv")

        
    }

    def generateSimilarItems(testDf:DataFrame, similarityDf:DataFrame,processedDf:DataFrame,method:String="cosine" ) = {
        
        val singleSample = testDf.limit(1)       
        print(s"Test Product Id:\n")
        singleSample.select("product_id","product_name").show()
        method match{
            case "cosine" => 
                println("\nproducts based on similarities \n")
                /*
                val simDf = similarityDf.alias("smdf")
                            .join(processedDf.select("product_id","product_name")
                                .withColumnRenamed("product_name","product_name_left")
                                .alias("fulldf"),col("smdf.product_id_left")=== col("fulldf.product_id"))
                            .drop(col("fulldf.product_id"))
                            .join(processedDf.select("product_id","product_name")
                                .withColumnRenamed("product_name","product_name_right")
                                .alias("fulldf2"),col("smdf.product_id_right")=== col("fulldf2.product_id"))
                            .drop(col("fulldf2.product_id"))
                */
                //simDf.show(10)
                val productsDf = objDataProcessing.readCSV("data/products.csv")
                val simDf = similarityDf.alias("smdf")
                            .join(productsDf.select("product_id","product_name")
                                .withColumnRenamed("product_name","product_name_left")
                                .alias("pdf"),col("smdf.product_id_left")=== col("pdf.product_id"))
                            .drop(col("pdf.product_id"))
                            .join(productsDf.select("product_id","product_name")
                                .withColumnRenamed("product_name","product_name_right")
                                .alias("pdf2"),col("smdf.product_id_right")=== col("pdf2.product_id"))
                            .drop(col("pdf2.product_id"))

                val sampleSimilarities = simDf.alias("smdf")
                                    .join(singleSample.select("product_id","product_name")
                                    .alias("singleSample"),
                                    col("smdf.product_id_right") === col("singleSample.product_id"),"inner")
                                    .select("product_id_left","product_id_right","product_name_left","product_name_right","cosine_similarity")
                                    .sort($"cosine_similarity".desc)
                        
                println("\nshowing top 30 similar products:\n")
                sampleSimilarities.show(30, false)
           
            case "cooccurance" => 
                    println("\nSimilar products based on Cooccurances \nOur Cooccurance Matrix: ")
                    similarityDf.show(10)
                    val sampleSimilarities = similarityDf.alias("smdf")
                                    .join(singleSample.select("product_id","product_name")
                                    .alias("singleSample"),
                                    col("smdf.product_id_right") === col("singleSample.product_id"),"inner")
                                    .drop("singleSample.product_id","singleSample.product_name")
                                    .sort($"cooccurances".desc)
                        
                println("\nshowing top 30 similar products:\n")
                sampleSimilarities.show(30, false)
                    
        }

    }
}