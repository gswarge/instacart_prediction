package main
import etl.objDataProcessing
import shapeless.Data
import org.sparkproject.dmg.pmml.True
import java.util.zip.DataFormatException
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating





object objItemMatrix {
    println("In ItemMatrix")
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings
 

    def generateItemItemMatrix(inputDf: DataFrame): DataFrame = {
        println("\nChecking Processed DataSet: ")
        inputDf.show(5)
        println("\nSelecting only required columns: user_id, product_id, order_id...")
        val subDf = inputDf.select("user_id","product_id", "order_id")
        
        subDf.show(10)
        //println("\nDataset RowCount: "+subDf.count)
        //println("\nDistinct RowCount: "+subDf.distinct.count)

        println  ("\nGenerating ItemItem matrix")
        val columnNames = subDf.select("product_id").distinct.map(
                        row => row(0).asInstanceOf[Int]).collect().toSeq
        println("\nColmnNames Length: "+columnNames.length)
         //setting max values for Pivot, since we have total 49689 products
        spark.conf.set("spark.sql.pivotMaxValues", columnNames.length)

        val output = subDf.drop("user_id")
                .withColumnRenamed("product_id","product_id_left")
                .as("df1")
                .join(subDf.as("df2"),$"df1.order_id" === $"df2.order_id")
                .withColumn("ones",lit(1))
                .withColumnRenamed("product_id","product_id_right")
                .drop("order_id","user_id")
                
        println("preprocessing before pivoting")
        output.show(10)
        val itemMatrixDf = output.groupBy("product_id_left")
                .pivot("product_id_right",columnNames)
                .count()
                .na.fill(0)

        println("\nMatrix Generated")
        itemMatrixDf
    }

    

    def generateUserItemMatrix(inputDf: DataFrame) : DataFrame = {
        /*
          Generate a userItemMatrix from dataframe 
         
        */
        println("\n**** Attempt to generate userItem Matrix **** \n")
        inputDf.show(5)
        println("\nSelecting only required columns: user_id, product_id, order_id...")
        val subDf = inputDf.select("user_id","product_id", "order_id")
        subDf.show(10)

        val columnNames = subDf.select("product_id").distinct.map(
                        row => row(0).asInstanceOf[Int]).collect().toSeq
        
        spark.conf.set("spark.sql.pivotMaxValues", columnNames.length)

        val output = subDf.withColumn("ones",lit(1))
                .drop("order_id")

        println("Prepossed before Pivoting")
        output.show(15)
        
        val userItemMatrixDf = output.groupBy("user_id")
                                .pivot("product_id",columnNames)
                                .count()
                                .na.fill(0)
       
        println("User Item Matrix generated")

        userItemMatrixDf
    }

    def generateNormalisedMatrix(inputDf: DataFrame): DataFrame ={

        println("\nGenerting Normalised matrix, \n\nStep1: Assembling feature columns as Vectors")
        val columnNames = inputDf.columns
        val assembler = new VectorAssembler()
            .setInputCols(columnNames)
            .setOutputCol("features")
        
        val output = assembler.transform(inputDf)
        println("\nAll columns combined to a vector column named 'features'\n")
        println("\nMatrix RowSize: "+output.count())
        
        val normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0)
        
        val l1NormData = normalizer.transform(output)
        println("\nNormalized using L^1 norm")
        l1NormData
    }
    
    

    def userItemMatrixAls(filteredDF: DataFrame) = {
        /*
            Using default Alternate Least Squares method provided in Spark
        */
        val userItemDf = filteredDF.groupBy("user_id","product_id").count()
        userItemDf.printSchema()
        userItemDf.show(25)
        val purchases = userItemDf.rdd.map( 
            row => Rating(row.getAs[Int](0), row.getAs[Int](1), row.getLong(2).toDouble))
        val rank = 25
        val numIterations = 10
        val model = ALS.train(purchases, rank, numIterations, 0.01)
        
        println("\nModel Ranks:\n"+model.rank)
        // Evaluate the model on purchase counts
        val usersProducts = purchases.map { 
            case Rating(user, product, purchaseCount) => (user, product)
        }

        println("\nUser Products:\n"+usersProducts.top(10))

        val predictions = model.predict(usersProducts).map { 
            case Rating(user, product, purchaseCount) =>
            ((user, product), purchaseCount)
        }
        println("\nPredictions:\n"+predictions.top(5))

        val ratesAndPreds = purchases.map { 
            case Rating(user, product, purchaseCount) =>((user, product), purchaseCount)
        }.join(predictions)
        
        println("\nRates &  Predictions:\n"+ratesAndPreds.top(10))

        val MSE = ratesAndPreds.map { 
            case ((user, product), (r1, r2)) => 
            val err = (r1 - r2) 
            err * err
        }.mean()
       
        println(s"\nMean Squared Error = $MSE\n")
        //Mean Squared Error = 0.6048095711284814
        //Mean Squared Error = 3.8308691544774995 for full dataset

        // Save and load model
        model.save(spark.sparkContext, "model/myALSModel")
        /*
        // Generate top 10 movie recommendations for each user
        val userRecs = model.recommendForAllUsers(10)
        // Generate top 10 user recommendations for each movie
        val movieRecs = model.recommendForAllItems(10)
        // Generate top 10 movie recommendations for a specified set of users
        val users = ratings.select(als.getUserCol).distinct().limit(3)
        val userSubsetRecs = model.recommendForUserSubset(users, 10)
        // Generate top 10 user recommendations for a specified set of movies
        val movies = ratings.select(als.getItemCol).distinct().limit(3)
        val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
        */

    }

}