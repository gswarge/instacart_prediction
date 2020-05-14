import numpy as np
import data as d
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Creating Spark Context
SC = SparkContext.getOrCreate()
SPARK = SparkSession(SC)

def main(): 
    """[Main Function]
    """
    #Load Cooccurance Matrix and Similarity Matrix
    filePath = "data/fullDfCooccurances.csv"
    fullCooccuranceMat = SPARK.read.csv(filePath,inferSchema=True,header=True)
    filePath = "data/productSimilarities.csv"
    fullSimMat = SPARK.read.csv(filePath,inferSchema=True,header=True)

    #Load Product_Ids for test
    filePath = "data/products.csv"
    sampleProductId = SPARK.read.csv(filePath,inferSchema=True,header=True)

    print ("Cooccurance Matrix:\n ")
    fullCooccuranceMat.show(10)
    print ("Similarity Matrix:\n ")
    fullSimMat.show(10)
    print ("Products: \n")
    sampleProductId.show(10)
    
if __name__ == "__main__":
    main()
