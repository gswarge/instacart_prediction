import numpy as np
import data as d
import pandas as pd
from collections import namedtuple
import time
from datetime import timedelta
import re
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
    inputBasketFilePath = "../../../data/queryProdIds.txt"
    with open(inputBasketFilePath,'r') as f:
        inputBasket = [int(l.strip()) for l in f]
    
    simMatfilePath = "../../../data/concatproductSims/allProdSims.txt"
    #======================================================
    #Method 1
    #method1_time = time.time()
    #method1(filePath,inputBasket)
    #print("--- method 1 run time (): ",(time.time()-method1_time)/60)
    
    #======================================================
    #method2_time = time.time()
    #method2(filePath, queryList)
    #print("--- method 2 run time ():" , (time.time()-method2_time)/60)
    
    #======================================================
    # Method 3: using a dicts for products
    method3_time = time.time()
    method3(simMatfilePath,inputBasket)
    d = timedelta(seconds=(time.time()-method3_time))
    print("--- method 3 run time (): ",d)
    
    

def method3(simMatfilePath,inputBasket):
    print("Method 3:\n")
    similarProducts = []
    prodDict = {}
    prodFilePath = "../../../data/products.csv"

    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})
    
    print("got the dicts")

    for prod_id in inputBasket:
        i=0
        print("Searching for prod_id: ", prod_id)
        file = open(simMatfilePath)
        for line in file:
            record = line.strip().split('|')
            if ((int(record[0]) == prod_id) & (i <= 3)):
                similarProducts.append(record)
                i+=1
                print("found ya:", record,prod_id)
                #print("i: ",i)                    
            if i >= 4:
                #print("found break: ")
                file.close()
                break
                

    print('\nResults are in!')
    df = pd.DataFrame.from_records(similarProducts, columns=['product_id_left','product_id_right','cosine_sims'])
    
    df = df.astype({'product_id_left': 'int64','product_id_right': 'int64','cosine_sims': 'float64'})
    
    df['product_name_left'] = df['product_id_left'].map(prodDict)
    df['product_name_right'] = df['product_id_right'].map(prodDict)

    for prod_id in inputBasket:
        print("Top 3 Similar Items to: ", prod_id, "\n",df[df['product_id_left']== prod_id].sort_values('cosine_sims',ascending=False)[1:])
    
    #print(df.head(15))
    print("Adios Amigoes")
        
        
   
    '''
    for line in simMat:
            row = line.strip().split('|')
            for prod_id in inputBasket:
                if int(record[1] == prod_id):
                        similarProducts.append(record)
                        print (record, line)
    '''
    

def method1(filePath,queryList): 
    records = []
    productsdf = pd.read_csv('../../../data/products.csv')
    productsdf = productsdf.astype({'product_id': 'int64','aisle_id': 'int64','department_id': 'int64','product_name':'object'})

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
    
    df = df.astype({'product_id_left': 'int64','product_id_right': 'int64','cosine_sims': 'float64'})

    df = df.merge(productsdf,
            left_on="product_id_left",right_on="product_id",how='inner',suffixes=('_left','_right')) \
            .drop(['product_id','aisle_id','department_id'],axis=1) \
            .merge(productsdf,
            left_on="product_id_right",right_on="product_id",how='inner',suffixes=('_left','_right'))\
            .drop(['product_id','aisle_id','department_id'],axis=1)

    
    for query in queryList:
        print("Top 3 Similar Items to: ", query, "\n",df[df['product_id_right']== int(query)].sort_values('cosine_sims',ascending=False).head(4)[1:])
    
    #print(df.head(15))
    print("Adios Amigoes")



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
    d = timedelta(seconds=(time.time()-start_time))
    print("--- total run time (): " , d)

