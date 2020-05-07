package main
import etl.objDataProcessing
import shapeless.Data
import org.sparkproject.dmg.pmml.True
import java.util.zip.DataFormatException
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.recommendation._


object objModels {
     val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext
    //==================================================================================================================
    //this method uses the inbuild Alternate Least Squares algorithm to generate the similarities & predictions.

    def applyItemItemALS(inputDf: DataFrame) = {
        val filteredDf = inputDf.select("product_id_left","product_id_right","cooccurances")
        inputDf.show(5)
        val ratings = filteredDf.rdd.map( 
            row => Rating(row.getAs[Int](0), row.getAs[Int](1), row.getLong(2).toDouble))
        val rank = 25
        val numIterations = 10
        val alsModel = ALS.train(ratings, rank, numIterations, 0.01)
        
        println("\nModel Ranks:\n"+alsModel.rank)

        // Evaluate the model on purchase counts
        val usersProducts = ratings.map { 
            case Rating(user, product, purchaseCount) => (user, product)
        }

        println("\nUser Products:\n"+usersProducts.take(5).toSeq)

        val predictions = alsModel.predict(usersProducts).map { 
            case Rating(user, product, purchaseCount) =>
            ((user, product), purchaseCount)
        }
        println("\nPredictions:\n"+predictions.take(5).toSeq)

        val ratesAndPreds = ratings.map { 
            case Rating(user, product, purchaseCount) =>((user, product), purchaseCount)
        }.join(predictions)
        
        println("\nRates &  Predictions:\n"+ratesAndPreds.take(5).toSeq)

        val MSE = ratesAndPreds.map { 
            case ((user, product), (r1, r2)) => 
            val err = (r1 - r2) 
            err * err
        }.mean()
       
        println(s"\nMean Squared Error = $MSE\n")
        //Mean Squared Error = 0.6048095711284814
        //Mean Squared Error = 3.8308691544774995 for full dataset

        // Save and load model
        //alsModel.save(spark.sparkContext, "model/alsModel")

    }
}

