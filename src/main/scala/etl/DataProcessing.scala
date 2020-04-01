package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data
import spire.std.`package`.string
import java.io.File

object DataProcessing {
    println("G")
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    def getParquet(parquetPath: String): DataFrame = {
        val spark = SparkSession.builder().getOrCreate()
        println("read parquet!")
        spark.read.parquet(parquetPath)
    }

    def readCSV(csvPath: String): DataFrame = {
        println("read csv!")
        val csvPath = "orders.csv"
        val spark = SparkSession.builder().getOrCreate()
        val csvDf = spark.read.format("csv").option(
            "header", "true").option(
            "inferSchema", "true").load(
            csvPath
        )
        csvDf
    }
    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        val okFileExtensions = List("csv")
        d.listFiles.filter(_.isFile).toList.filter { 
            file => okFileExtensions.exists(file.getName.endsWith(_))
        }
    }

    def loadData () = {
        println ("Loading Data....")
        
        val csvPath = List("data/orders.csv","data/aisles.csv","data/departments.csv","data/products.csv","data/order_products_prior.csv","data/order_products_train.csv")

        val ordersDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(0))
        println("Loaded "+ csvPath(0))
        ordersDF.printSchema()

        val aislesDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(1))
        println("Loaded "+ csvPath(1))
        aislesDF.printSchema()

        val departmentsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(2))
        println("Loaded "+ csvPath(2))
        departmentsDF.printSchema()

        val productsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(3))
        println("Loaded "+ csvPath(3))
        productsDF.printSchema()

        var oppDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(4))
        println("Loaded "+ csvPath(4))
        oppDF.printSchema()

        var optDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(5))
        println("Loaded "+ csvPath(5))
        optDF.printSchema()

        //combine products with departments and aisles
        var productDfFinal = mergeData(productsDF,aislesDF, "aisle_id")
        println("first merge: products and aisles tables...")
        productDfFinal.printSchema()
        productDfFinal = mergeData(productDfFinal,departmentsDF,"department_id")
        println("second merge products and departments...")
        productDfFinal.printSchema()
        // Merge orders and order products train & prior with orders table
        println("Merging order_products_train with orders df")
        optDF = mergeData(optDF,ordersDF,"order_id")
        optDF.printSchema()
        println("Merging order_products_prior with orders df")
        oppDF = mergeData(oppDF,ordersDF,"order_id")
        oppDF.printSchema()

    }

    def mergeData(df1: DataFrame,df2: DataFrame, key :String): DataFrame = {
        println("Merging Data... ")
    return df1.join(df2, df1(key) === df2(key), joinType="inner")
    }
    
    def writeToCSV(df: DataFrame, fileName: String): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        println("writing dataframe to csv!")
        df.write.format("com.databricks.spark.csv").option("header", "true").save(fileName)
    }

}