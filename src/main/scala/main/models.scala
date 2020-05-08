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

    def applyItemItemALS(inputDf: DataFrame,testItemsDf:DataFrame) = {
        val filteredDf = inputDf.select("product_id_left","product_id_right","cooccurances")
        inputDf.show(5)
        val ratings = filteredDf.rdd.map( 
            row => Rating(row.getAs[Int](0), row.getAs[Int](1), row.getLong(2).toDouble))
        // rank is the number of features to use (also referred to as the number of latent factors).
        val rank = 25
        val numIterations = 10
        val alsModel = ALS.train(ratings, rank, numIterations, 0.18)
        
        println("\nModel Ranks:\n"+alsModel.rank)

        // Evaluate the model on purchase counts
        val itemProducts = ratings.map { 
            case Rating(item, product, purchaseCount) => (item, product)
        }

        println("\nUser Products:\n"+itemProducts.take(5).toSeq)

        val predictions = alsModel.predict(itemProducts).map { 
            case Rating(item, product, purchaseCount) =>
            ((item, product), purchaseCount)
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

    def applySVD(cooccuranceMat:CoordinateMatrix,testItemsDf:DataFrame) = {   
        
        val m = cooccuranceMat.numRows()
        val n = cooccuranceMat.numCols()
        val mat = cooccuranceMat.toIndexedRowMatrix()
        
        // Compute 10 largest singular values and corresponding singular vectors
        val svd: SingularValueDecomposition[IndexedRowMatrix, Matrix] = mat.computeSVD(10, computeU = true)
        val U: IndexedRowMatrix = svd.U  // The U factor is a IndexedRowMatrix.
        val s: Vector = svd.s     // The singular values are stored in a local dense vector.
        val V: Matrix = svd.V     // The V factor is a local dense matrix.

        svd.s.toArray.zipWithIndex.foreach { 
                case (x, y) => println(s"Singular value #$y = $x") }

        val maxRank = Seq(mat.numCols(), mat.numRows()).min
        val total = svd.s.toArray.map(x => x * x).reduce(_ + _)
        val worstLeft = svd.s.toArray.last * svd.s.toArray.last * (maxRank - svd.s.size)
        val variabilityGrasped = 100 * total / (total + worstLeft)

        println(s"Worst case variability grasped: $variabilityGrasped%")

        println(U)
        println(V)

    }
}

