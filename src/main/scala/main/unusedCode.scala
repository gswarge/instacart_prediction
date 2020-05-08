package main
import etl.objDataProcessing

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow,IndexedRowMatrix,RowMatrix}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.ml.linalg.{Vectors => NewVectors}

import shapeless.Data
import org.apache.spark.sql.functions._



object objUnusedCode {
     val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings
    val sqlContext = spark.sqlContext

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
    //Ver 1, Method 2: Using Row Matrix , works but i again lose the index values, plus this method needs the input as Matrix, which means again Pivot on the full dataset needs to work 

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

    def mainMethod() ={
        // Code from Main method 

    //========================================================================
    //step 4: Generate ItemItemMatrix
    //val itemMatrixDf = objItemMatrix.generateItemItemMatrix(processedDf)
    //or
    //val itemMatrixDf = objDataProcessing.readCSV("data/ItemItemMatrix.csv")
    
    //========================================================================
    //Step 5: Generate UserItemMatrix
    //val userItemMatrixDf = objItemMatrix.generateUserItemMatrix(processedDf)
    //objDataProcessing.writeToParquet(itemMatrixDf,"data/fullUserItemMatrix.parquet")

    //Using Spark's ALS algorithm
    //val userItemMatridDf = objItemMatrix.userItemMatrixAls(processedDf)

    //========================================================================
    //Step 6: Normalise generated Matrix
    //val normalisedItemMatrix = objItemMatrix.generateNormalisedMatrix(itemMatrixDf)
    //val normalisedUserItemMatrix = objItemMatrix.generateNormalisedMatrix(userItemMatridDf)
    
    
    }
}