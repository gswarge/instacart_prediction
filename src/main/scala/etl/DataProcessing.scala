package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProcessing {
    println("G")
    val spark = SparkSession.builder().getOrCreate()

    def getParquet(parquetPath: String): DataFrame = {
        val spark = SparkSession.builder().getOrCreate()
        println("read parquet!")
        spark.read.parquet(parquetPath)
    }

    def readCSV(csvPath: String): DataFrame = {
        println("read csv!")
        val csvPath = "orders.csv"
        val csvDf = spark.read.format("csv").option(
            "header", "true").option(
            "inferSchema", "true").load(
            csvPath
        )
        print(csvDf)
        csvDf
    }

    def loadData () = {
        println ("Loading Data....")
        val csvPath = List("orders.csv","aisles.csv","departments.csv","products.csv","order_products_prior.csv")

        val ordersDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(0))
        println("Loaded "+ csvPath(0))
        val aislesDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(1))
        println("Loaded "+ csvPath(1))
        val departmentsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(2))
        println("Loaded "+ csvPath(2))
        val productsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(3))
        println("Loaded "+ csvPath(3))
        val oppDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(4))
        println("Loaded "+ csvPath(4))
            
    }

    // "⁨/Users/eric/Dropbox/SharpestMinds/Gaurang/Mentorship/instacart_prediction/data/order_products__prior.csv"

    def writeToCSV(df: DataFrame, fileName: DataFrame): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        println("writing dataframe to csv!")
        df.write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")
    }

    /*
    def someML(filePath: String): Unit = {
        val df = readCSV

        ...
        DF1 + T1 -> DF2 + T2 -> ... -> DFinal
        ...
        val mlPipeline = Pipeline(T1, T2, ...)
        val DFinal = runPipeline(DF1, mlPipeline)
        
        writeToCSV(DFinal)
    }
    */
}