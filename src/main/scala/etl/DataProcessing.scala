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
        
        val (trainOrdersDf, testOrdersDf) = processOrdersData()
        processPriorAndLastOrders(trainOrdersDf,testOrdersDf)
        //val productsDfFinal = processProductsDataset()
         
    }

    def processOrdersData():Tuple2[DataFrame,DataFrame] = {
        
        println("Loading.... "+ csvPath(0))
        val ordersDF = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(0))
        println("Loaded.... "+ csvPath(0))
        //Split orders data in training and test set
        //Get list of user_id's included in test set
        val origTestUserIds = ordersDF.filter(ordersDF("eval_set") === "test").select("user_id")
        // Get list of user IDs included in train set
        val origTrainUserIds = ordersDF.filter(ordersDF("eval_set") === "train").select("user_id")
        
        val totalUserCount = ordersDF.groupBy("user_id").count().count()
        

        println("\n\n**** Checking Train and Test Set ****\n")
        println("Total Users in dataset: "+totalUserCount)
        println("Users in original train set: "+origTrainUserIds.count())
        println("Users in original test set: "+origTestUserIds.count())
        println("\n****Splitting the dataset into 80:20 ratio - training:test****\n")
        
        
        val userCountInOrigTrainSet = origTrainUserIds.count()
        val userCountInOrigTestSet = origTestUserIds.count()
        val userCountNewTestSet = math.floor(totalUserCount/5).toLong // 20%
        val userCountNewTrainSet = totalUserCount - userCountNewTestSet
        // creating validation set from our new train set
       // val userCountValSet = math.floor(userCountNewTrainSet/5).toLong // 20%

        println("User count in New train set: "+userCountNewTrainSet)
        println("User count in New test set: "+userCountNewTestSet)
        
        val tempTrainUserIds = ordersDF.groupBy("user_id").count().limit(userCountNewTrainSet.toInt)
        
        val testOrders = ordersDF.join(tempTrainUserIds,ordersDF("user_id")===tempTrainUserIds("user_id"),"left_anti").drop("count")
        val trainOrders = ordersDF.join(testOrders,ordersDF("user_id")===testOrders("user_id"),"left_anti") 
        
        println("\nTotal Orders in Training dataset: "+trainOrders.count())
        println("Total Users in Training Orders dataset:"+trainOrders.groupBy("user_id").count().count())
        println("Total Orders in Test dataset: "+testOrders.count())
        println("Total Users in Test Orders dataset:"+testOrders.groupBy("user_id").count().count())

        //writeToCSV(trainOrders,"data/processed/trainOrders.csv")   
        //writeToCSV(testOrders,"data/processed/testOrders.csv")   

        (trainOrders,testOrders)
    }

    def processProductsDataset() : DataFrame ={

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
        writeToCSV(productDfFinal,"data/processed/productsDfFinal.csv")
        productDfFinal
    }

    def processPriorAndLastOrders(trainOrdersDf:DataFrame,testOrdersDf:DataFrame)={

        // Order_products_prior means the users prior orders
        println("Loading.... "+ csvPath(4))
        val priorUserOrdersDf = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(4))
        println("Loaded.... "+ csvPath(4))
        
        println("Loading.... "+ csvPath(5))
        
        // Order_products_train contains the users last or latest order
        val lastUserOrdersDf  = spark.read.format("csv").option(
                        "header", "true").option("inferSchema", "true").load(
                            csvPath(5))
        println("Loaded.... "+ csvPath(5))
        
        // I dont want a training and validation set, plus a test set since im doing a recommender system
        // I want a training and test set seperately, since i have last orders already given, i only need to use the prior orders to generate cosine similarity so that i can compare recommendations with the last orders from "order_products_train.csv".
        //orders prior table contains the details of the orders prior to that users most recent order, where as orders table consist details of all the orders without product ids or product information at the same time specifying whether this order exist in test, prior or train set

        println("Merging prior orders with training set")
        
        // ordersDF.join(tempTrainUserIds,ordersDF("user_id")===tempTrainUserIds("user_id"),"left_anti").drop("count")
        //priorUserOrdersDf.join()
        val trainSetPriorOrderProduct = priorUserOrdersDf.filter(priorUserOrdersDf.("order_id") === trainOrdersDf("order_id"))
        println("row count Train Set Prior Orders: "+trainSetPriorOrderProduct.count())
        //writeToCSV(trainSetPriorOrderProduct,"data/processed/trainSetPriorOrderProduct.csv")

        
        println("Merging prior orders with test set")
        val testSetPriorOrderProduct = priorUserOrdersDf.filter(priorUserOrdersDf("order_id") === testOrdersDf("order_id"))
        println("row count test Set Prior Orders: "+testSetPriorOrderProduct.count())
        //writeToCSV(testSetPriorOrderProduct,"data/processed/testSetPriorOrderProduct.csv")
        

        println("Merging last orders with training set")
        val trainSetLastOrderProduct = lastUserOrdersDf.filter(lastUserOrdersDf("order_id") === trainOrdersDf("order_id"))
        //writeToCSV(trainSetLastOrderProduct,"data/processed/trainSetLastOrderProduct.csv")

        println("Merging last orders with test set")
        val testSetLastOrderProduct = lastUserOrdersDf.filter(lastUserOrdersDf("order_id") === testOrdersDf("order_id"))
       // writeToCSV(testSetLastOrderProduct,"data/processed/testSetLastOrderProduct.csv")
       

        //(trainSetPriorOrderProduct,testSetPriorOrderProduct,trainSetLastOrderProduct,testSetLastOrderProduct)

       /*
        
        val orderProductsDF = mergeDf(priorOrdersDf,ordersDF,"order_id","outer")
        
       
        //Merging orderProductsDf with productsDf to have department and aisles in the same dataframe
        println("\nMerging orderProductsDf with  productsDf\n")
        println("\norderProductsDf count:"+orderProductsDF.count())
        val fullOrderProductsDf = mergeDf(orderProductsDF,productsDF,"product_id","outer")
        println("\nFull orderProductsDf count:"+fullOrderProductsDf.count())
        println("Schema of Full Dataset:\n"+fullOrderProductsDf.printSchema())
        println(fullOrderProductsDf.show(10))
        
        println(s"\nSaving Full Dataframe at $fullProcessedDfPath")
        writeToParquet(fullOrderProductsDf,fullProcessedDfPath)        


    /*  // Merging train and orders database
        println("Merging order_products_train with orders df")
        optDF = mergeDf(optDF,ordersDF,"order_id")
        optDF.printSchema()
        optDF.show()
    */ 
    
        fullOrderProductsDf
     */
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