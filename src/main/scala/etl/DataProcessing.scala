package etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data
import spire.std.`package`.string
import java.io.File
import spire.syntax.`package`.order
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix



object objDataProcessing {
    
    val spark = SparkSession
        .builder()
        .appName("Instacart Prediction Project")
        .config("spark.master", "local[*]")
        .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    
    val csvPath = List("data/original/orders.csv",
                    "data/original/aisles.csv",
                    "data/original/departments.csv",
                    "data/original/products.csv",
                    "data/original/order_products_prior.csv",
                    "data/original/order_products_train.csv")

    def ingestAndProcessData(fullProcessedDfPath: String) = {
        println ("\n******Loading Data******\n")
        
        val (lastOrdersOfUsers, priorOrdersOfUsers) = processOrdersData()
        val productsDfFinal = processProductsDataset()
        processPriorAndLastOrders(lastOrdersOfUsers,priorOrdersOfUsers,productsDfFinal)    
    }

    def processOrdersData():Tuple2[DataFrame,DataFrame] = {
        
        println("Loading.... "+ csvPath(0))
        val ordersDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(0))
        println("Loaded.... "+ csvPath(0))
        
        //Checking no of users in test, train(last) and prior orders set
        val origTestUserIds = ordersDF.filter(ordersDF("eval_set") === "test").select("user_id").groupBy("user_id").count()
        val origTrainUserIds = ordersDF.filter(ordersDF("eval_set") === "train").select("user_id").groupBy("user_id").count()
        val origPriorUserIds = ordersDF.filter(ordersDF("eval_set") === "prior").select("user_id").groupBy("user_id").count()
        
        val totalUserCount = ordersDF.groupBy("user_id").count().count()
        

        println("\n\n**** Checking Train and Test Set ****\n")
        println("Total Users in dataset: "+totalUserCount)
        println("Users in original train set: "+origTrainUserIds.count())
        println("Users in original test set: "+origTestUserIds.count())
        println("Users in original prior set: "+origPriorUserIds.count())
        

        val lastOrdersOfUsers = ordersDF.filter(ordersDF("eval_set") === "train")
        val priorOrdersOfUsers = ordersDF.filter(ordersDF("eval_set") === "prior")
        val testOrdersDf = ordersDF.filter(ordersDF("eval_set") === "test")

        writeToCSV(lastOrdersOfUsers,"data/processed/lastOrdersOfUsers.csv")   
        writeToCSV(priorOrdersOfUsers,"data/processed/priorOrdersOfUsers.csv")
        writeToCSV(testOrdersDf,"data/processed/testOrders.csv")   
        
        (lastOrdersOfUsers,priorOrdersOfUsers)
    }

    

    def processPriorAndLastOrders(lastOrdersOfUsers:DataFrame,priorOrdersOfUsers:DataFrame,productsDfFinal:DataFrame)={

        // Order_products_prior means the users prior orders
        println("Loading.... "+ csvPath(4))
        val priorOrdersProdDf = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(4))
        println("Loaded.... "+ csvPath(4))
        
        println("Loading.... "+ csvPath(5))
        
        // Order_products_train contains the users last or latest order
        val lastOrdersProdDf  = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(5))
        println("Loaded.... "+ csvPath(5))
        
        println("\nMerging order_products_prior with prior_orders set")
        println("prior order_product count: "+priorOrdersProdDf.count())
        val temp = mergeDf(priorOrdersProdDf,priorOrdersOfUsers,"order_id","inner")
        println("count post merge: "+temp.count())

        println("\nMerging product data with PriorOrders\n")
        val allPriorOrderProducts = mergeDf(temp,productsDfFinal,"product_id","left")
        println("count post merge: "+allPriorOrderProducts.count())
        allPriorOrderProducts.head()


        println("Merging order_products_train with last_orders set")
        println("order_product_train count: "+lastOrdersProdDf.count())
        val temp2 = mergeDf(lastOrdersProdDf,lastOrdersOfUsers,"order_id","inner")
        println("count post merge: "+temp2.count())
        
        println("\nMerging product data with lastOrders\n")
        val allLastOrderProducts = mergeDf(temp2,productsDfFinal,"product_id","left")
        println("count post merge: "+allLastOrderProducts.count())
        allLastOrderProducts.head()

        writeToCSV(allPriorOrderProducts,"data/processed/allPriorOrderProductsUsers.csv")   
        writeToCSV(allLastOrderProducts,"data/processed/allLastOrderProductsUsers.csv")   
        
    }

    def processProductsDataset(): DataFrame ={

        println("Loading.... "+ csvPath(1))
        val aislesDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(1))
        println("Loaded.... "+ csvPath(1))
        
        println("Loading.... "+ csvPath(2))
        val departmentsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(2))
        println("Loaded.... "+ csvPath(2))
        
        println("Loading.... "+ csvPath(3))
        val productsDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(3))
        println("Loaded.... "+ csvPath(3))
        
        // Merging Aisles and Products
        println("Merging Aisles and Products dataset ... ")
        val productDf1 = mergeDf(productsDF,aislesDF, "aisle_id","inner")
        println("Merged....")
        println("second merge products & aisles with departments...")
        val productDfFinal = mergeDf(productDf1,departmentsDF,"department_id","inner")
        println("Merged....")
        writeToCSV(productDfFinal,"data/processed/allProducts.csv")
        productDfFinal
    }

    def mergeDf(df1: DataFrame,df2: DataFrame, key :String, joinType:String): DataFrame = {
        //Merge dataframes and remove duplicate columns post merging, Inner join is the default join in Spark
        //val finalDF = df1.alias("df1").join(df2.alias("df2"), key).drop(df2(key))
        val finalDF = df1.join(df2,Seq(key),joinType)
        finalDF
    }
   

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
        
        println("writing  csv file at..."+fileName)
        df.write.format("com.databricks.spark.csv").option("header", "true").save(fileName)
        println("csv file written at... "+fileName)
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
        println(s"\nLoading  $parquetPath:")
        val spark = SparkSession.builder().getOrCreate()
        spark.read.parquet(parquetPath)
    }

    def readCSV(csvPath: String): DataFrame = {
        println(s"reading csv file:$csvPath")
        val spark = SparkSession.builder().getOrCreate()
        val csvDf = spark.read.format("csv").option(
            "header", "true").option(
            "inferSchema", "true").load(
            csvPath
        )
        csvDf
    }

    def writeToText(df:DataFrame,savePath:String) = {
        println(s"writing text file:$savePath")
        //df.write.text(csvPath)
        df.rdd.map(x=>x(0)+"|"+x(1)+"|"+x(2)).saveAsTextFile(savePath)
        println("file written !")
    }


    def getListOfFiles(dir: String):List[File] = {    
        val d = new File(dir)
        val okFileExtensions = List("csv")
        d.listFiles.filter(_.isFile).toList.filter { 
            file => okFileExtensions.exists(file.getName.endsWith(_))
        }
    }

    def saveSimMatrix(savePath: String, sim: CoordinateMatrix): Unit = {
        sim.entries.map(x=>x.i+"|"+x.j+"|"+x.value).coalesce(1).saveAsTextFile(savePath)

  }
}