package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data
import spire.std.`package`.string
import java.io.File
import spire.syntax.`package`.order


object objDataProcessing {
    println("In DataProcessing")
    
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") //To avoid warnings

    def ingestAndProcessData(): DataFrame = {
        println ("\n******Loading Data******\n")
        val csvPath = List("data/orders.csv","data/aisles.csv","data/departments.csv","data/products.csv","data/order_products_prior.csv","data/order_products_train.csv")


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
        
        val ordersDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(0)).cache()
        println("Loaded "+ csvPath(0))
        ordersDF.printSchema()
        ordersDF.show(5)

        
        val oppDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(4))
        println("Loaded "+ csvPath(4))
        oppDF.printSchema()
    /* 
        //Load orderProducts train 
        val optDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(5))
        println("Loaded "+ csvPath(5))
        optDF.printSchema()    
    */
        // Merging Aisles and Products
        println("\nProducts Count: "+productsDF.count())
        val productDf1 = mergeDf(productsDF,aislesDF, "aisle_id","inner")
        println("\nProducts & Aisles Count: "+productDf1.count())
        productDf1.printSchema()
        println("second merge products and departments...\nProducts DF final")
        val productDfFinal = mergeDf(productDf1,departmentsDF,"department_id","inner")
        println("\nProducts Final Count: "+productDfFinal.count())
        productDfFinal.printSchema()
        productDfFinal.show(5)

        println("Merging order_products_prior with orders df")
        //orders prior table contains the details of the orders prior to that users most recent order, where as orders table consist details of only the orders without product ids or product information
        println("\nOrder_products_prior count:"+oppDF.count())
        val orderProductsDF = mergeDf(oppDF,ordersDF.select("user_id","order_id","order_dow","order_hour_of_day","order_number"),"order_id","outer")
        println("\nMerged Opp count:"+orderProductsDF.count())
        orderProductsDF.printSchema()
        orderProductsDF.show()

        //Merging orderProductsDf with productsDf to have department and aisles in the same dataframe
        println("\nMerging orderProductsDf with  productsDf\n")
        println("\norderProductsDf count:"+orderProductsDF.count())
        val fullOrderProductsDf = mergeDf(orderProductsDF,productsDF,"product_id","outer")
        println("\nFull orderProductsDf count:"+fullOrderProductsDf.count())
        println("Schema of Full Dataset:\n"+fullOrderProductsDf.printSchema())
        println(fullOrderProductsDf.show(10))
        println("\nSaving Full Dataframe...")
        writeToParquet(fullOrderProductsDf,"data/fullOrderProductsDf.parquet")        


    /*  // Merging train and orders database
        println("Merging order_products_train with orders df")
        optDF = mergeDf(optDF,ordersDF,"order_id")
        optDF.printSchema()
        optDF.show()
    */ 
        //createFilteredDF(productDfFinal,orderProductsDF,5)
        fullOrderProductsDf
    }

    def mergeDf(df1: DataFrame,df2: DataFrame, key :String, joinType:String): DataFrame = {
        //Merge dataframes and remove duplicate columns post merging, Inner join is the default join in Spark
        val colNames = df2.columns.toSeq
        //val finalDF = df1.alias("df1").join(df2.alias("df2"), key).drop(df2(key))
        val finalDF = df1.join(df2,Seq(key),joinType)
        println(finalDF.columns.toSeq)
    return finalDF
    }
   
    /* 
        Need to merge orders.csv, opp.csv first, then based on the product_ids in alcohol df, pickup the rows from the merged dataframe
    */

    def generateRandomSample(inputDf: DataFrame, samplePercentage:Double = 0.10): DataFrame = {
        
        val filteredDf = inputDf.sample(true, samplePercentage)
        filteredDf
    }

    def generateDepWiseSample(inputDf: DataFrame,columnName: String = "department_id", departmentId:Int = 5): DataFrame = {
        
        val filteredDf = inputDf.filter(inputDf(columnName) === departmentId)
        filteredDf
    }


    def writeToCSV(df: DataFrame, fileName: String): Unit = {
        /*
            Write to a CSV File
        */
        val spark = SparkSession.builder().getOrCreate()
        println("writing dataframe to csv!")
        df.write.format("com.databricks.spark.csv").option("header", "true").save(fileName)
        println("csv file written!")
    }

    def writeToParquet(df: DataFrame, filePath:String): Unit = {
        /*
            Write to Parquet File
        */
        println(s"Writing file at $filePath")
        df.write.parquet(filePath)
        println("file written")
    }

    def getParquet(parquetPath: String): DataFrame = {
        val spark = SparkSession.builder().getOrCreate()
        println("reading parquet file!")
        spark.read.parquet(parquetPath)
    }

    def readCSV(csvPath: String): DataFrame = {
        println("read csv!")
        val spark = SparkSession.builder().getOrCreate()
        val csvDf = spark.read.format("csv").option(
            "header", "true").option(
            "inferSchema", "true").load(
            csvPath
        )
        csvDf
    }
    def getListOfFiles(dir: String):List[File] = {
        /*
            Function to get the list of csv files in the given directory
        */
        val d = new File(dir)
        val okFileExtensions = List("csv")
        d.listFiles.filter(_.isFile).toList.filter { 
            file => okFileExtensions.exists(file.getName.endsWith(_))
        }
    }
}