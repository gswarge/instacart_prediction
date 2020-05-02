package main
import etl.DataProcessing
import shapeless.Data
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.sparkproject.dmg.pmml.True
import java.util.zip.DataFormatException
import org.apache.spark.sql.functions._


object ItemMatrixFunc {
    println("In ItemMatrix")

    def loadProcessedData(inputPath: String){
        println("Here we go, Showtime !")

        val filteredDf = DataProcessing.getParquet(inputPath)
        filteredDf.printSchema()
        
        val subDf = filteredDf.select("user_id","product_id", "order_id")

        subDf.show(10)
        print(subDf.count)
        print(subDf.distinct.count)

        val dfBasketJoin = generateItemItem(subDf)

        // dfBasketJoin.show(1600)
        dfBasketJoin.show(25)
        print(dfBasketJoin.count)
        
        //userItemMatrix(filteredDf)
        //userItemMatrixAls(filteredDf)
        //itemItemMatrix(filteredDf)
        
    }

    def generateItemItem(inputDf: DataFrame): DataFrame = {
        
        val filteredDf = inputDf.sample(true, 0.1)

        val dfOriginal = filteredDf.withColumnRenamed(
            "user_id", "user_id_1").withColumnRenamed(
            "product_id", "product_id_1").withColumnRenamed(
            "order_id", "order_id_1")
        
        val dfMirror = filteredDf.withColumnRenamed(
            "user_id", "user_id_2").withColumnRenamed(
            "product_id", "product_id_2").withColumnRenamed(
            "order_id", "order_id_2")
        
        val dfBasketJoin = dfOriginal.join(
            dfMirror, 
            dfOriginal("user_id_1") === dfMirror("user_id_2") && dfOriginal("order_id_1") === dfMirror("order_id_2"), 
            "left_outer").withColumn(
            "ones", lit(1))
        
        val dfBasketJoin = dfOriginal.join(
            dfMirror, 
            dfOriginal("order_id_1") === dfMirror("order_id_2"), 
            "left_outer").withColumn(
            "ones", lit(1))
        
        val ii1 = dfBasketJoin.select("user_id_1", "user_id_2", "product_id_1", "product_id_2", "order_id_1", "order_id_2", "ones")
        val ii2 = ii1.filter(col("product_id_1") =!= col("product_id_2"))
        
        dfBasketJoin

    }

    def userItemMatrixAls(filteredDF: DataFrame) = {
        /*
            Using default Alternate Least Squares method provided in MLlib library of Spark
        */
        val userItemDf = filteredDF.groupBy("user_id","product_id").count()
        userItemDf.printSchema()
        userItemDf.show(25)
        val purchases = userItemDf.rdd.map( 
            row => Rating(row.getAs[Int](0), row.getAs[Int](1), row.getLong(2).toDouble))
        val rank = 10
        val numIterations = 10
        val model = ALS.train(purchases, rank, numIterations, 0.01)
        
        println("\nModel Ranks:\n"+model.rank)
        // Evaluate the model on purchase counts
        val usersProducts = purchases.map { 
            case Rating(user, product, purchaseCount) => (user, product)
        }

        println("\nUser Products:\n"+usersProducts.top(5))

        val predictions = model.predict(usersProducts).map { 
            case Rating(user, product, purchaseCount) =>
            ((user, product), purchaseCount)
        }
        println("\nPredictions:\n"+predictions.top(5))

        val ratesAndPreds = purchases.map { 
            case Rating(user, product, purchaseCount) =>((user, product), purchaseCount)
        }.join(predictions)
        
        println("\nRates &  Predictions:\n"+ratesAndPreds.top(5))

        val MSE = ratesAndPreds.map { 
            case ((user, product), (r1, r2)) => 
            val err = (r1 - r2) 
            err * err
        }.mean()
       
        println(s"\nMean Squared Error = $MSE\n")
        //Mean Squared Error = 0.6048095711284814

    }

    def userItemMatrix(filteredDf: DataFrame) = {
        /*
          Generate a userItemMatrix from dataframe and calculate similarties between each rows (ie users) using cosine similarity
         
        */
        println("\n**** Attempt to generate userItem Matrix & calculating cosine similarities **** \n")
       
        val userItemDf = filteredDf.groupBy("user_id").pivot("product_id").count()
        
        
        println("\nUserItemMatrix rowLength: "+ userItemDf.count()+" | UserItemMatrix columnLength: " + userItemDf.columns.size)
        val userItemDf2 = userItemDf.na.fill(0)
        //DataProcessing.writeToCSV(userItemDf,"data/userItemDf.csv")
       
        val columnNames = userItemDf.columns.drop(1)//dropping user_id as column
        //println("\ncolumn length: "+columnNames.length+" Column Names:\n"+columnNames.toSeq)
        println("Assembling Vectors")
        val assembler = new VectorAssembler()
            .setInputCols(columnNames)
            .setOutputCol("productsPurchased")
        
        val output = assembler.transform(userItemDf2)
        println("\nAll columns combined to a vector column named 'productsPurchased'\n")
        output.select("user_id","productsPurchased").show(5)
        println("\nMatrix RowSize: "+output.select("user_id","productsPurchased").count())
        
        //val outputRdd = output.select("user_id","productsPurchased").rdd.map{
            //row => Vectors.dense(row.getAs[Seq[Double]](1).toArray)
            //row.getAs[Vector](1)
        val outputRdd = output.select("user_id","productsPurchased").rdd.map{
            row => Vectors.dense(row.getAs[Seq[Double]](1).toArray)
            }
        print("\n RDD size count: "+outputRdd.count())
        val prodPurchasePerUserRowMatrix = new RowMatrix(outputRdd)
            //http://spark.apache.org/docs/latest/ml-migration-guides.html
        // Compute cosine similarity between columns 

        val simsCols = prodPurchasePerUserRowMatrix.columnSimilarities()
        println("\nNo. of Cols: "+simsCols.numCols()+ "\n No. of Rows: "+ simsCols.numRows())
        
    }

    def itemItemMatrix(filteredDf: DataFrame) = {

        println("\n**** Attempt to generate Item-Item Matrix **** \n")
    
        //val itemItemDf = filteredDf.groupBy("product_id").pivot("product_id").count()
        //println("ItemMatrix rowLength: "+ itemItemDf.count()+" | ItemMatrix columnLength: " + itemItemDf.columns.size)

        val fpgrowth = new FPGrowth().setItemsCol("product_id").setMinSupport(0.5).setMinConfidence(0.6)
        val model = fpgrowth.fit(filteredDf)

        // Display frequent itemsets.
        model.freqItemsets.show()

        // Display generated association rules.
        model.associationRules.show()

        // transform examines the input items against all the association rules and summarize the consequents as prediction
       
        model.transform(filteredDf).show()

        
       // DataProcessing.writeToCSV(itemItemDf,"data/itemItemDf.csv")
    

    }

    def fpGrowthTest()={
/*
        val dataset = spark.createDataset(Seq(
        "1 2 5",
        "1 2 3 5",
        "1 2")
        ).map(t => t.split(" ")).toDF("items")

        val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
        val model = fpgrowth.fit(dataset)

        // Display frequent itemsets.
        model.freqItemsets.show()

        // Display generated association rules.
        model.associationRules.show()

        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        model.transform(dataset).show()
*/
    }
}

/*
            Logic 1:
            In order to create User-Item Matrix, I need to convert our dataset into a matrix with the product names(or IDs) as the columns, the user_id as the index and the count of product_ids as the values. By doing this we shall get a dataframe with the columns as the product names and the rows for each user ids. Each column represents all the purchases of a product by all users in the dataset. The purchases appear as NAN where a user didn't purchase a certain product. 

            We should be able use this matrix to compute the correlation between the purchases of a single product and the rest of the products in the matrix.
            this can be achieved via groupby and pivot

            Logic 2:
            For item-item matrix, each product_id is a column as well as a row and the value at the correlation is the no of times the products were  purchased together.
            logic: for each orderID, increment the (prod1,prod2) value in the matrix
            Diagonals of the matrix will be the max no of times the product is purchased.
            For an item-similarity based recommender:
                1.Represent all items by a feature vector
                2. Construct an item-item similarity matrix by computing a similarity metric (such as cosine) with each items pair
                3.Use this item similarity matrix to find similar items for users

        */