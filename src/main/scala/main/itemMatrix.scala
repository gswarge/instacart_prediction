package main
import etl.DataProcessing
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data
import org.sparkproject.dmg.pmml.True
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import java.util.zip.DataFormatException


object ItemMatrixFunc {
    println("In ItemMatrix")

    def generateMatrixDf(){
        println("Here we go, Showtime !")
        val filteredDF = DataProcessing.getParquet("data/filteredDF.parquet")
        filteredDF.printSchema()
        filteredDF.select("user_id","product_id","order_id").show(10)
        println(filteredDF.count(), filteredDF.columns.size)

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
        val userItemDf = filteredDF.groupBy("user_id").pivot("product_id").count()
        println("UserItemMatrix rowLength: "+ userItemDf.count()+" | UserItemMatrix columnLength: " + userItemDf.columns.size)
    /*
            (153696,13)
            UserItemMatrix rowLength: 15798 | UserItemMatrix columnLength: 1055
    */
    /*
     
        val itemItemDf = filteredDF.groupBy("product_id").pivot("product_id").count()
        println("ItemMatrix rowLength: "+ itemItemDf.count()+" | ItemMatrix columnLength: " + itemItemDf.columns.size)

        //DataProcessing.writeToCSV(itemItemDf,"data/itemItemDf.csv")
        //DataProcessing.writeToCSV(userItemDf,"data/userItemDf.csv")
    */    
        //userItemMatrix(userItemDf)
        //itemItemMatrix(itemItemDf)
        
        val newfilteredDF=filteredDF.withColumn("product_id_2",filteredDF("product_id"))
        newfilteredDF.select("user_id","product_id","order_id","product_id_2").show(10)       
        val itemItemMatrix = newfilteredDF.select("user_id","order_id","product_id","product_id_2").stat.crosstab("product_id", "product_id_2")
        //val userItemMatrix = filteredDF.select("user_id","order_id","product_id").stat.crosstab("product_id", "user_id")
        DataProcessing.writeToCSV(itemItemMatrix,"data/itemItemDf.csv")
        //DataProcessing.writeToCSV(itemItemMatrix,"data/userItemDf.csv")

    }

    def userItemMatrix(userItemDF: DataFrame) = {
        /*
          Generate a userItemMatrix from dataframe and calculate similarties between each rows (ie users) using cosine similarity
          Apply cosineSimilarity function to each row?
        */
        
        userItemDF.na.fill(0)
        println ("Calculating Cosine Similarity")
        //userItemDF
        
        
    }

    def itemItemMatrix(itemItemDF: DataFrame) = {
        /*
            val mat = new CoordinateMatrix(ratings.map {
                  case Rating(user, movie, rating) => MatrixEntry(user, movie, rating)
                })
        */

    }
}