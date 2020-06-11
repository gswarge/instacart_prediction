package main
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.ml.linalg.{Vectors => NewVectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import etl.objDataProcessing
import org.apache.spark.sql.functions._
import shapeless.record


/*
    Scala Object for calculating Cosine Similarity
*/
object objCosineSimilarity {

    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext
    //========================================================================
    //Ver 1, Method 1: THIS METHOD WORKS, USE THIS METHOD,
    // NO NEED TO PIVOT TO CREATE MATRIX, AS THIS WORKS ON SELFJOIN
    // Implementing Cosine Similarity formula:  (x dot y) / ||X|| ||Y||

    def generateCosineSimilarties(inputDf: DataFrame, savePath:String): DataFrame = {
        println("Selecting user_id, order_id, product_id..")
        val filteredDf = inputDf
                        .select("user_id","product_id","order_id")
                        .withColumn("ones",lit(1))

        val numerator = filteredDf
                    .withColumnRenamed("product_id","product_id_left")
                    .as("df1")
                    .join(
                        filteredDf
                        .withColumnRenamed("product_id","product_id_right")
                        .as("df2"))
                    .where($"df1.user_id" === $"df2.user_id" && 
                    $"df1.order_id" === $"df2.order_id"  )
                    .groupBy("product_id_left","product_id_right")
                    .agg(sum($"df1.ones" * $"df2.ones").alias("dot"))

        println("Numerator calculated... ")
        //numerator.show(5)

        val magnitude = filteredDf
                    .groupBy("product_id")
                    .agg(sqrt(sum($"ones" * $"ones")).alias("norm"))

        //println("Norms: ")
        //magnitude.show(10)
        
        val cosine = ($"dot" / ($"magnitudeX.norm" * $"magnitudeY.norm")).as("cosine_similarity") 

        val similaritiesDf = numerator
                .join(
                    magnitude
                    .alias("magnitudeX"),
                    $"magnitudeX.product_id" === $"product_id_left")
                .join(
                    magnitude
                    .alias("magnitudeY"),
                    $"magnitudeY.product_id" === $"product_id_right")
                .select($"product_id_left", $"product_id_right", cosine)
                .sort($"cosine_similarity".desc)

                
        println("Similarities calculated...")

        //similaritiesDf.show(25)
        //similaritiesDf.filter("cosine_similarity > 1").show(25)
        objDataProcessing.writeToText(similaritiesDf,"data/processed/allPriorOrdersProductsSimilarities.txt")
        similaritiesDf

    }
       

   /*
        * This method takes 2 equal length arrays of integers 
        * It returns a double representing similarity of the 2 arrays
        * 0.9925 would be 99.25% similar
        * (x dot y)/||X|| ||Y||
   */
   
    def calculateCosineSimilarity(x: Array[Int], y:Array[Int]): Double = {
        require(x.size == y.size)
        dotProduct(x,y)/(magnitude(x) * magnitude(y))

    }

    /*
        Return Dot Product of 2 Arrays
         e.g. (a[0]*b[0])+(a[1]*a[2])
    */
    def dotProduct(x: Array[Int], y:Array[Int]): Int = {
        (for ((a,b) <- x zip y) yield a * b ) sum
    }

    /*
        Return Magnitude of an Array
        We multiply each element, sum it, then square root the result.
    */
    def magnitude(x: Array[Int]): Double = {
        math.sqrt(x map (i => i*i) sum)

    }
    
}