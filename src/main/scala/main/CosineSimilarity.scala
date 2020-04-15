package main

/*
    Scala Object for calculating Cosine Similarity
*/
object CosineSimilarity {

    /*
        * This method takes 2 equal length arrays of integers 
        * It returns a double representing similarity of the 2 arrays
        * 0.9925 would be 99.25% similar
        * (x dot y)/||X|| ||Y||
   */
    def cosineSimilarity(x: Array[Int], y:Array[Int]): Double = {
        require(x.size == y.size)
        dotProduct(x,y)/(magnitude(x) * magnitude(y))

    }

    /*
        Return Dot Product of 2 Arrays
         e.g. (a[0]*b[0])+(a[1]*a[2])
    */
    def dotProduct(x: Array[Int], y:Array[Int]): Int = {
        (for ((a,b) <- x zip y) yield a * b ) sum
    }

    /*
        Return Magnitude of an Array
        We multiply each element, sum it, then square root the result.
    */
    def magnitude(x: Array[Int]): Double = {
        math.sqrt(x map (i => i*i) sum)

    }
/*
    import pyspark.sql.functions as func

    def cosine_similarity(df, col1, col2):
        df_cosine = df.select(func.sum(df[col1] * df[col2]).alias('dot'), 
                          func.sqrt(func.sum(df[col1]**2)).alias('norm1'), 
                          func.sqrt(func.sum(df[col2] **2)).alias('norm2'))
        d = df_cosine.rdd.collect()[0].asDict()
    return d['dot']/(d['norm1'] * d['norm2'])

    cosine_similarity(df, 'a', 'b') # output 0.989949

*/
    
}