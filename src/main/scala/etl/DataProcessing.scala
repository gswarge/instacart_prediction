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
    //Loading Data in Main object, so that i have access to dataframes in all functions
    println ("\n******Loading Data******\n")
    val csvPath = List("data/orders.csv","data/aisles.csv","data/departments.csv","data/products.csv","data/order_products_prior.csv","data/order_products_train.csv")

    val ordersDF = spark.read.format("csv").option(
                    "header", "true").option("inferSchema", "true").load(
                        csvPath(0)).cache()
    println("Loaded "+ csvPath(0))
    ordersDF.printSchema()

    val aislesDF = spark.read.format("csv").option(
                    "header", "true").option("inferSchema", "true").load(
                        csvPath(1))
    println("Loaded "+ csvPath(1))
    //aislesDF.printSchema()

    val departmentsDF = spark.read.format("csv").option(
                    "header", "true").option("inferSchema", "true").load(
                        csvPath(2))
    println("Loaded "+ csvPath(2))
    //departmentsDF.printSchema()

    val productsDF = spark.read.format("csv").option(
                    "header", "true").option("inferSchema", "true").load(
                        csvPath(3))
    println("Loaded "+ csvPath(3))
    //productsDF.printSchema()

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
    var productDfFinal = mergeDf(productsDF,aislesDF, "aisle_id")

    def processData () = {
        println("combine products with departments and aisles")    
        println("first merge: products and aisles tables...")
        productDfFinal.printSchema()
        productDfFinal = mergeDf(productDfFinal,departmentsDF,"department_id")
        println("second merge products and departments...\nProducts DF final")
        productDfFinal.printSchema()
        productDfFinal.show()
        println("Merging order_products_train with orders df")
        optDF = mergeDf(optDF,ordersDF,"order_id")
        optDF.printSchema()
        optDF.show()
        println("Merging order_products_prior with orders df")
        //orders prior table contains the orders prior to that users most recent order(present in orders table)
        oppDF = mergeDf(oppDF,ordersDF.select("user_id","order_id"),"order_id")
        oppDF.printSchema()
        oppDF.show()
    }

    def mergeDf(df1: DataFrame,df2: DataFrame, key :String): DataFrame = {
        println("Merging Data... ")
    return df1.join(df2, df1(key) === df2(key), joinType="inner")
    }
    /*
        group by products in order id and count will give top selling products
        group by user id and count(order_id) will give users with most orders and users with their order count
        select only product orders from order_products_prior for those users whose data is in order_products_train

    */
    def eda() = {
        productDfFinal.createOrReplaceGlobalTempView("products")
        oppDF.createOrReplaceGlobalTempView("orders_prior")
        var output = spark.sql("'select COUNT(global_temp.order_id) from global_temp.orders_prior GROUP BY global_temp.product_id'")
        output.show()

    }




    def writeToCSV(df: DataFrame, fileName: String): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        println("writing dataframe to csv!")
        df.write.format("com.databricks.spark.csv").option("header", "true").save(fileName)
    }
    
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
}