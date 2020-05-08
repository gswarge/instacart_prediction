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
import org.apache.spark.mllib.recommendation._


object objItemMatrix {
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings
    val sqlContext = spark.sqlContext

//==================================================================================================================
//Method to generate a Item-Item Cooccurances

def generateCooccurances(inputDf: DataFrame,savePath:String): Tuple2[DataFrame,CoordinateMatrix] = {
    val filteredDf = inputDf
                    .select("user_id","product_id","order_id","product_name")
                    .withColumn("ones",lit(1))

    val cooccuranceDf = filteredDf
                .withColumnRenamed("product_id","product_id_left")
                .withColumnRenamed("product_name","product_name_left")
                .as("df1")
                .join(
                    filteredDf
                    .withColumnRenamed("product_id","product_id_right")
                    .withColumnRenamed("product_name","product_name_right")
                    .as("df2"))
                .where($"df1.user_id" === $"df2.user_id" && 
                $"df1.order_id" === $"df2.order_id")
                .groupBy("product_id_left","product_id_right","product_name_left","product_name_right")
                .agg(sum($"df1.ones").alias("cooccurances"))
                                     
   val temp = cooccuranceDf.select("product_id_left","product_id_right","cooccurances")
            .rdd.map{
                row => MatrixEntry(row(0).asInstanceOf[Int],row(1).asInstanceOf[Int],row.getLong(2).toDouble)
            }
        
        println("Mapped to MatrixEntry")    
        println(temp.count())
    val cooccuranceMat = new CoordinateMatrix(temp)

    println(s"CoOrdinate Matrix Dimensions: $cooccuranceMat.numCols(),$cooccuranceMat.numRows()")
    cooccuranceDf.where($"product_id_left" =!= $"product_id_right").sort($"cooccurances".desc).show(10,false)
    //objDataProcessing.writeToCSV(cooccuranceDf,savePath)
    (cooccuranceDf,cooccuranceMat)
}



//==================================================================================================================
//Method to generate a Item-Item Matrix by pivoting the dataframe 

    def generateItemItemMatrix(inputDf: DataFrame): DataFrame = {
        println("\nChecking Processed DataSet: ")
        inputDf.show(5)
        println("\nSelecting only required columns: user_id, product_id, order_id...")
        val subDf = inputDf.select("user_id","product_id", "order_id")
        val filteredDf = inputDf
                        .select("user_id","product_id","order_id")
                        .withColumn("ones",lit(1))
        
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
                .join(
                    subDf.as("df2"),
                    $"df1.order_id" === $"df2.order_id" && $"df1.user_id" === $"df2.user_id")
                .withColumn("ones",lit(1))
                .withColumnRenamed("product_id","product_id_right")
                .drop("order_id","user_id")
                
        println("preprocessing before pivoting")
        output.show(10)
        val itemMatrixDf = output.groupBy("product_id_left")
                .pivot("product_id_right",columnNames)
                .count()
                .na.fill(0)
                .orderBy("product_id_left")

        println("\nMatrix Generated")
        itemMatrixDf
    }

    
//==================================================================================================================
//Method to generate a User-Item Matrix by pivoting the dataframe 

    def generateUserItemMatrix(inputDf: DataFrame) : DataFrame = {
        
        inputDf.show(5)
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

    //==================================================================================================================
    //This method normalises the matrix using Normalizer from the ml library
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
    

}