import numpy as np
import data as d
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import namedtuple
import time
#from pyspark.sql.functions import col
#from pyspark.sql.types import *
#from pyspark import SparkContext
#from pyspark.sql import SparkSession

# Creating Spark Context
#SC = SparkContext.getOrCreate()
#SPARK = SparkSession(SC)

def main(): 
    """[Main Function]
    """
    print('ShowTime!')
    #Load query prod_ids and Similarity Matrix
    filePath = "data/queryProdIds.txt"
    with open(filePath,'r') as f:
        queryList = [l.strip() for l in f]
    
    productsdf = pd.read_csv('data/products.csv')
    
    filePath = "data/concatproductSims/allProdSims.txt"
    method1_time = time.time()
    method1(filePath,queryList,productsdf)
    print("--- method 1 run time (): ",(time.time()-method1_time)/60)
    #--- method 1 run time ():  4.049646683533987
    
    #method2_time = time.time()
    #method2(filePath, queryList)
    #print("--- method 2 run time ():" , (time.time()-method2_time)/60)
    #--- method 2 run time (): 4.074459417661031
    
def method1(filePath,queryList,productsdf): 
    records = []
    with open(filePath,'r') as f:
        for line in f:
            record = line.strip().split('|')
            for query in queryList:
                if int(record[1] == query):
                    if float(record[2]) > 0.05:
                        records.append(record)
                        #print (query, line)
    print('\nResults are in!')
    df = pd.DataFrame.from_records(records, columns=['product_id_left','product_id_right','cosine_sims'])
    
    #df = df.astype({'product_id_left': 'int64','product_id_right': 'int64','cosine_sims': 'float64'})

    #df = df.merge(
    #        productsdf,
    #        left_on="product_id_left",right_on="product_id",how="inner",sort=False) \
    #        .drop('product_id',axis=1)
    #df.sort_values('cosine_sims')
    #print(df.shape)
    #print(df.head(50))
    for query in queryList:
        print("Top 3 Similar Items to: ", query, "\n",df[df['product_id_right']== query].sort_values('cosine_sims',ascending=False).head(4)[1:])



def method2(filePath,queryList):
    records = []
    file = open(filePath)
    while 1:
        lines = file.readlines(1000000)
        if not lines:
            break
        for line in lines:
            record = line.strip().split('|')
            for query in queryList:
                if int(record[1] == query):
                    if float(record[2]) > 0.05:
                        records.append(record)
    print('\nResults are in!')
    df = pd.DataFrame.from_records(records, columns=['product_id_left','product_id_right','cosine_sims'])
    df.sort_values('cosine_sims')
    print(df.shape)
    print(df.head(50))

    
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- total run time (): " , (time.time()-start_time)/60)

