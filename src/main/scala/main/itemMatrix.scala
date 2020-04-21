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

    def loadProcessedData(inputPath: String="data/filteredf.parquet"){
        
        println("Loading filtered dataset of department Alcohol:")
        val filteredDf = objDataProcessing.getParquet(inputPath)
        filteredDf.printSchema()
        filteredDf.show(15)
        filteredDf.count()

        println("Selecting 3 columns:")
        val subDf = filteredDf.select("user_id","product_id", "order_id")
        subDf.show(10)
        println("\nFilteredDf Count:"+subDf.count)
        println("\nFilteredDf Distinct Count:"+subDf.distinct.count)

        val itemMatrixDf = generateItemItemMatrix(subDf)
        //objDataProcessing.writeToCSV(itemMatrixDf,"data/ItemItemMatrix.csv")
        //val itemMatrixDf = objDataProcessing.readCSV("data/ItemItemMatrix.csv")
        println(itemMatrixDf.count)
        
        val normalisedMatrix = generateNormalisedMatrix(itemMatrixDf)

        

       
    }
    def generateItemItemMatrix(inputDf: DataFrame): DataFrame = {
        println  ("\nGenerating ItemItem matrix")
        val itemMatrixDf = inputDf.drop("user_id")
                .withColumnRenamed("product_id","product_id_left")
                .as("df1")
                .join(inputDf.as("df2"),$"df1.order_id" === $"df2.order_id")
                .withColumn("ones",lit(1))
                .withColumnRenamed("product_id","product_id_right")
                .drop("order_id","user_id")
                .groupBy("product_id_left")
                .pivot("product_id_right")
                .count()
                .na.fill(0)

        println("\nMatrix Generated:")
        println(itemMatrixDf.count())

        itemMatrixDf
    }

    def generateNormalisedMatrix(inputDf: DataFrame): DataFrame ={

        println("Assembling Vectors")
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
        println("Normalized using L^1 norm")
        l1NormData
    }

    def userItemMatrixAls(filteredDF: DataFrame) = {
        /*
            Using default Alternate Least Squares method provided in MLlib library of Spark
        */
        val userItemDf = filteredDF.groupBy("user_id","product_id").count()
        userItemDf.printSchema()
        userItemDf.show(25)
        val purchases = userItemDf.rdd.map( 
            row => Rating(row.getAs[Int](0), row.getAs[Int](1), row.getLong(2).toDouble))
        val rank = 10
        val numIterations = 10
        val model = ALS.train(purchases, rank, numIterations, 0.01)
        
        println("\nModel Ranks:\n"+model.rank)
        // Evaluate the model on purchase counts
        val usersProducts = purchases.map { 
            case Rating(user, product, purchaseCount) => (user, product)
        }

        println("\nUser Products:\n"+usersProducts.top(5))

        val predictions = model.predict(usersProducts).map { 
            case Rating(user, product, purchaseCount) =>
            ((user, product), purchaseCount)
        }
        println("\nPredictions:\n"+predictions.top(5))

        val ratesAndPreds = purchases.map { 
            case Rating(user, product, purchaseCount) =>((user, product), purchaseCount)
        }.join(predictions)
        
        println("\nRates &  Predictions:\n"+ratesAndPreds.top(5))

        val MSE = ratesAndPreds.map { 
            case ((user, product), (r1, r2)) => 
            val err = (r1 - r2) 
            err * err
        }.mean()
       
        println(s"\nMean Squared Error = $MSE\n")
        //Mean Squared Error = 0.6048095711284814

    }

    def generateUserItemMatrix(filteredDf: DataFrame) = {
        /*
          Generate a userItemMatrix from dataframe and calculate similarties between each rows (ie users) using cosine similarity
         
        */
        println("\n**** Attempt to generate userItem Matrix & calculating cosine similarities **** \n")
       
        val userItemDf = filteredDf.groupBy("user_id").pivot("product_id").count()
        
        
        println("\nUserItemMatrix rowLength: "+ userItemDf.count()+" | UserItemMatrix columnLength: " + userItemDf.columns.size)
        val userItemDf2 = userItemDf.na.fill(0)
        //DataProcessing.writeToCSV(userItemDf,"data/userItemDf.csv")
       
        val columnNames = userItemDf.columns.drop(1)//dropping user_id as column
        //println("\ncolumn length: "+columnNames.length+" Column Names:\n"+columnNames.toSeq)
        println("Assembling Vectors")
        val assembler = new VectorAssembler()
            .setInputCols(columnNames)
            .setOutputCol("productsPurchased")
        
        val output = assembler.transform(userItemDf2)
        println("\nAll columns combined to a vector column named 'productsPurchased'\n")
        output.select("user_id","productsPurchased").show(5)
        println("\nMatrix RowSize: "+output.select("user_id","productsPurchased").count())
        
        //val outputRdd = output.select("user_id","productsPurchased").rdd.map{
            //row => Vectors.dense(row.getAs[Seq[Double]](1).toArray)
            //row.getAs[Vector](1)
        val outputRdd = output.select("user_id","productsPurchased").rdd.map{
            row => Vectors.dense(row.getAs[Seq[Double]](1).toArray)
            }
        print("\n RDD size count: "+outputRdd.count())
        val prodPurchasePerUserRowMatrix = new RowMatrix(outputRdd)
            //http://spark.apache.org/docs/latest/ml-migration-guides.html
        // Compute cosine similarity between columns 

        val simsCols = prodPurchasePerUserRowMatrix.columnSimilarities()
        println("\nNo. of Cols: "+simsCols.numCols()+ "\n No. of Rows: "+ simsCols.numRows())
        
    }
}