from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession



# Creating Spark Context
SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

def loadData():
    """
    Load Instacart Dataset
    """
    ordersdf = SPARK.read.csv('data/orders.csv',inferSchema=True,header=True)
    productsdf = SPARK.read.csv('data/products.csv',inferSchema=True,header=True)
    depdf = SPARK.read.csv('data/departments.csv',header=True, inferSchema=True)
    aislesdf = SPARK.read.csv('data/aisles.csv',header=True,inferSchema=True)
    
    print("\nThe are total {} products in the dataset".format(productsdf.count()))
    print("\nProducts are categorised into {} departments".format(depdf.count()))
    print("\nProducts are categorised into {} aisles".format(aislesdf.count()))
    merged_productsdf = mergeDataset(productsdf, aislesdf, depdf)
    return ordersdf, merged_productsdf

   
def mergeDataset(productsdf, aislesdf, depdf):
    """[Merges products, aisles and department datasets into one single]
    
    Arguments:
        productsdf {[dataframe]} -- [products dataset]
        aislesdf {[type]} -- [description]
        depdf {[type]} -- [description]
    
    Returns:
        [dataframe] -- [merged dataframe]
    """ 
    merged_productsdf = productsdf.join(aislesdf, on='aisle_id')
    merged_productsdf = merged_productsdf.join(depdf,on='department_id')
    return merged_productsdf

#  a custom function to convert the data type of DataFrame columns
def convertColumn(df, names, newType):
  for name in names: 
     df = df.withColumn(name, df[name].cast(newType))
  return df 

def loadCSV(filePath):
    df = SPARK.read.csv(filePath,inferSchema=True,header=True)
    return df 
