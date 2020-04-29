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


/*
    Scala Object for calculating Cosine Similarity
*/
object objCosineSimilarity {

    /*
        * This method takes 2 equal length arrays of integers 
        * It returns a double representing similarity of the 2 arrays
        * 0.9925 would be 99.25% similar
        * (x dot y)/||X|| ||Y||
   */
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext

    def generateCosineSimilarity(inputDf: DataFrame,savePath:String): DataFrame = {

        println("\nGenerating Cosine Similarities...")
        val columnNames = inputDf.columns
        val assembler = new VectorAssembler()
            .setInputCols(columnNames)
            .setOutputCol("features")
            
        val output = assembler.transform(inputDf)

        output.select("product_id_left","features").show(5)
        val rowMat = output.select("features").rdd.map(
            _.getAs[org.apache.spark.ml.linalg.Vector](0)).map(
                org.apache.spark.mllib.linalg.Vectors.fromML)

        val matrix = new RowMatrix(rowMat)

        val similaritiesMatrix = matrix.columnSimilarities()
        
        objDataProcessing.saveSimMatrix(savePath,similaritiesMatrix)

        //println("Pairwise similarities are: " +   similaritiesMatrix.entries.collect.mkString(", "))
        println(similaritiesMatrix.entries.first())

        val transformedRDD = similaritiesMatrix.entries.map(
             x => (x.i,x.j,x.value)
            )

        val similaritiesDf = sqlContext.createDataFrame(transformedRDD).toDF("product_id_left", "product_id_right", "similarity")
        
        println(similaritiesDf.show(10))
        similaritiesDf
    }

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