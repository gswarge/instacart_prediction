package main
import etl.DataProcessing
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import shapeless.Data
import org.sparkproject.dmg.pmml.True


object itemMatrixFunc {
    println("In ItemMatrix")

    def createItemMatrixDF(){
        println("Here we go, Showtime !")
        val filteredDF = DataProcessing.getParquet("data/filteredOrderes.parquet")
        filteredDF.printSchema()
        filteredDF.show(15)
        println(filteredDF.count(), filteredDF.columns.size)
        
        /*
        //checking for any non alcohol department orders, to be sure
        val emptyOrdersDf = filteredDF.filter(filteredDF("department_id") =!= 5)
        emptyOrdersDf.show(15)
        */
        
        filteredDF.groupBy("product_id").count().show(15)
       
       //println("Dataframe Count:",temp.count(),"No of Columns: ",temp.columns.size)

        /*
            Logic 1:
            In order to create Item to Item Matrix, I need to convert our dataset into a matrix with the product names(or IDs) as the columns, the user_id as the index and the count of product_ids as the values. By doing this we shall get a dataframe with the columns as the product names and the rows for each user ids. Each column represents all the purchases of a product by all users in the dataset. The purchases appear as NAN where a user didn't purchase a certain product. 
            We can use this matrix to compute the correlation between the purchases of a single product and the rest of the products in the matrix.
            
            Logic 2:
            For item matrix, i need product_ids as columns and count of product ids as the values (each product_id is actually a purchase)

            
             group by product_id
            root
                |-- product_id: integer (nullable = true)
                |-- order_id: integer (nullable = true)
                |-- add_to_cart_order: integer (nullable = true)
                |-- reordered: integer (nullable = true)
                |-- user_id: integer (nullable = true)
                |-- department_id: string (nullable = true)
                |-- aisle_id: string (nullable = true)
                |-- product_name: string (nullable = true)
                |-- aisle: string (nullable = true)
                |-- department: string (nullable = true)
            (66101,3)
            +----------+-----+
            |product_id|count|
            +----------+-----+
            |     10206|    2|
            |     20135|    9|
            |     20683|   24|
            |     23336|   64|
            |     32445|   26|
            |     40515|   20|
            |     42635|  113|
            |     43852|  127|
            |      2387|   13|
            |      4929|   55|
            |     21715|  280|
            |     21859|   15|
            |      1699|    9|
            |     14997|   20|
            |       580|   14|
            +----------+-----+
            only showing top 15 rows
            (Dataframe Count:,1054,No of Columns,2)
            )
        */
        val productIDs = filteredDF.select("product_id").distinct().collect().seq
        //println(productIDs)
        //println("Size: "+productIDs.size)
        val itemMatrix = filteredDF.groupBy("product_id").pivot("product_id").count()
        //itemMatrix.printSchema()
        println("ItemMatrix rowLength: "+ itemMatrix.count()+" | ItemMatrix columnLength: " + itemMatrix.columns.size)
        //itemMatrix.describe()
        //itemMatrix.show(20,true)
        val userItemMatrix = filteredDF.groupBy("user_id").pivot("product_id").count()
        println("UserItemMatrix rowLength: "+ userItemMatrix.count()+" | UserItemMatrix columnLength: " + userItemMatrix.columns.size)

            
    


    }

}