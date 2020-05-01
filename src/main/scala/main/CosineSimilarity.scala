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
    //Ver 1, Method 1: THIS METHOD WORKS, AMAZING , USE THIS METHOD,
    // NO NEED TO PIVOT TO CREATE MATRIX, AS THIS WORKS ON SELFJOIN
    //// (x dot y)/||X|| ||Y||

    def generateCosineSimilartyWithoutMatrix(inputDf: DataFrame, savePath:String): DataFrame = {

        val filteredDf = inputDf
                        .select("user_id","product_id","order_id")
                        .withColumn("ones",lit(1))

        val numerator = filteredDf
                    .withColumnRenamed("product_id","product_id_left")
                    .withColumnRenamed("user_id","user_id_left")
                    .withColumnRenamed("order_id","order_id_left")
                    .as("df1")
                    .join(
                        filteredDf
                        .withColumnRenamed("product_id","product_id_right")
                        .withColumnRenamed("user_id","user_id_right")
                        .withColumnRenamed("order_id","order_id_right")
                        .as("df2"))
                    .where($"df1.user_id_left" === $"df2.user_id_right" && 
                    $"df1.order_id_left" === $"df2.order_id_right"  )
                    .groupBy("product_id_left","product_id_right")
                    .agg(sum($"df1.ones" * $"df2.ones").alias("dot"))

        //println("Numerator: ")
        //numerator.show(5)

        val norms = filteredDf
                    .groupBy("product_id")
                    .agg(sqrt(sum($"ones" * $"ones")).alias("norm"))

        //println("Norms: ")
        //norms.show(5)
        
        val cosine = ($"dot" / ($"this_norm.norm" * $"other_norm.norm")).as("cosine_similarity") 

        val similaritiesDf = numerator
                .join(
                    norms
                    .alias("this_norm"),
                    $"this_norm.product_id" === $"product_id_left")
                .join(
                    norms
                    .alias("other_norm"),
                    $"other_norm.product_id" === $"product_id_right")
                .select($"product_id_left", $"product_id_right", cosine)

        println("Similarities:")
        similaritiesDf.show(25)
        //similaritiesDf.filter("cosine_similarity > 1").show(25)

        //println("generated similarities")
        //objDataProcessing.writeToCSV(similaritiesDf,"data/productSimilarities.csv")
        objDataProcessing.writeToParquet(similaritiesDf,"data/productSimilarities.parquet")
        similaritiesDf

    }
         //========================================================================
        //Ver 2: Method 1: Using IndexedRowMatrix, without using VectorAssembler, this method works but i lose index of product id's,
        //though this method needs input as Matrix, which means pivot needs to work on full dataset
 
    def generateCosineSimilartyVer2 (inputMatrixDf: DataFrame,savePath:String): IndexedRowMatrix = {

        println(inputMatrixDf.select(array(inputMatrixDf.columns.tail.map(col): _*)))
        
        val simMat = new IndexedRowMatrix(inputMatrixDf
                    .select(array(inputMatrixDf.columns.tail.map(col): _*))
                    .rdd
                    .zipWithIndex
                    .map {
                        case (row, idx) => 
                        new IndexedRow(idx, OldVectors.dense(
                                (row.getSeq[Int](0).toArray)
                                .map(_.asInstanceOf[Int].toDouble)
                                )
                        )
                    })
                    //(for (i <- 0 to row.size) yeild row.getInt(i).toDouble).toArray
                    //row.getSeq[Double](0).toArray
                    //getSeq[Double]
        simMat.toCoordinateMatrix.transpose
                    .toIndexedRowMatrix.columnSimilarities
                    .toBlockMatrix.toLocalMatrix
        
        //Save the Matrix in TextFile
        //objDataProcessing.saveSimMatrix(savePath,simMat.toCoordinateMatrix())

        simMat
    }

    //========================================================================
    //Ver 1: Method 1: Trying to use custom cosine similarity function on the features column : Didnt work yet
    def generateCosineSimilarity(inputDf: DataFrame,savePath:String): DataFrame = {

        println("\nGenerating Cosine Similarities...")

        val columnNames = inputDf.columns
        val assembler = new VectorAssembler()
            .setInputCols(columnNames)
            .setOutputCol("features")
            
        val output = assembler.transform(inputDf)

        output.select("product_id_left","features").show(5)

        val customSimMatrix = output.as("a")
                    .crossJoin(output.as("b"))
                    .where($"a.product_id_left" === $"b.product_id_left")

        
        customSimMatrix.select("product_id_left","product_id_right","features")
        //val features = customSimMatrix.select(collect_list("features")).first().getList[Double](0)
        //calculateCosineSimilarity($"a.(collect_list(features)).first().getList[Double](0)",$"b.(collect_list(features)).first().getList[Double](0)")
        //customSimMatrix.withColumn("similarities",calculateCosineSimilarity(customSimMatrix.select("features"), customSimMatrix("features")))

        println("Similarities Matrix:")
        customSimMatrix.show(10)

    //========================================================================
    //Ver 1, Method 2: Using Row Matrix , works but i lose the index values 

        val rowMat = output.select("features").rdd.map(
            _.getAs[org.apache.spark.ml.linalg.Vector](0)).map(
                org.apache.spark.mllib.linalg.Vectors.fromML)

        val matrix = new RowMatrix(rowMat)

        val similaritiesMatrix = matrix.columnSimilarities()
        
        objDataProcessing.saveSimMatrix(savePath,similaritiesMatrix)

        //println("Pairwise similarities are: " +   similaritiesMatrix.entries.collect.mkString(", "))
        //println(customSimMatrix.entries.first())

        val transformedRDD = similaritiesMatrix.entries.map(
             x => (x.i,x.j,x.value)
            )

        val similaritiesDf = sqlContext.createDataFrame(transformedRDD).toDF("product_id_left", "product_id_right", "similarity")
        
        
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
    //========================================================================
    //How would i implement this in Python?  
      /*
        import pyspark.sql.functions as func

        def cosine_similarity(df, col1, col2):
            df_cosine = df.select(func.sum(df[col1] * df[col2]).alias('dot'), 
                            func.sqrt(func.sum(df[col1]**2)).alias('norm1'), 
                            func.sqrt(func.sum(df[col2] **2)).alias('norm2'))
            d = df_cosine.rdd.collect()[0].asDict()
        return d['dot']/(d['norm1'] * d['norm2'])

        cosine_similarity(df, 'a', 'b') # output 0.989949

    */
}