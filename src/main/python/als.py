import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
import time
#import csv
from datetime import timedelta
import implicit 
import glob
import os
from scipy.sparse.coo import coo_matrix
import logging
from predictions import calculate_MAP
log = logging.getLogger("implicit")


def main():
    path = "../../../data/processed/allPriorOrderProductsUsers.csv"
    ratingsPath = '../../../data/processed/ratingMatrix.csv'
    all_files = glob.glob(os.path.join(path, "*.csv"))    
    allPriorOrders   = pd.concat((pd.read_csv(f,usecols=['user_id','product_id','order_id']) for f in all_files), ignore_index=True)

    #generateRatings(allPriorOrders)
    alsModel(ratingsPath)
    

def generateRatings(allPriorOrders):
    filepath='../../../data/processed/ratingMatrix.csv'
    allPriorOrders['ones'] = 1
    print("ones created")
    ratingMatrix = allPriorOrders.merge(
                allPriorOrders,left_on=['user_id','order_id'],
                right_on=['user_id','order_id'],suffixes=('_left','_right'),sort=False)
    #print(ratingMatrix.head(10))
    print("grouping by")
    ratingMatrix = ratingMatrix.groupby(['user_id','product_id_left']).agg(purchases=('ones_left','sum')).sort_values(by='purchases',ascending=False).reset_index()
    print("ratings generated")
    
    #ratingMatrix = ratingMatrix.groupby(['user_id','product_id_left'],as_index=False)
    #ratingMatrix.sum().reset_index()[['user_id','product_id_left','ones_left']].to_csv('../../../data/processed/ratingMatrix.csv')
    ratingMatrix.to_csv(filepath)
    print("csv exported")
    #print(ratingMatrix.head(10))
    return

def alsModel(ratingsPath):
    print("\n***Generating Recommendations using ALS model***")
    userlist = [56886,71026,73136,88837,125582,25403,36860,70402,132121,4363,56592,83665,87073,99945]
    model_name = "Alternating Least Squares"
    ratingsMatrix = pd.read_csv(ratingsPath)
    ratingsMatrix.rename(columns={'product_id_left':'product_id','ones_left':'purchases'},inplace=True)
    
    # map each Item and user to a unique numeric value
    ratingsMatrix['user_id'] = ratingsMatrix['user_id'].astype("category")
    ratingsMatrix['product_id'] = ratingsMatrix['product_id'].astype("category")
    
    # create a sparse matrix of all the artist/user/play triples
    purchases = coo_matrix((ratingsMatrix['purchases'].astype(float), 
                   (ratingsMatrix['product_id'].cat.codes, 
                    ratingsMatrix['user_id'].cat.codes)))
    
    # initialize a model
    print("Initializing model")
    model = implicit.als.AlternatingLeastSquares(factors=50)
    
    # train the model on a sparse matrix of item/user/confidence weights
    log.debug("training model %s", model_name)
    start = time.time()
    model.fit(purchases)
    log.debug("trained model '%s' in %s", model_name, time.time() - start)
    log.debug("calculating top purchases")
    
    # recommend items for a user
    user_items = purchases.T.tocsr()
    #recommendations = model.recommend(userid, user_items)
    recommendations =[]
    for userid in userlist:
        for itemid, score in model.recommend(userid, user_items):
            record = [userid,itemid,score]
            recommendations.append(record)
            #print("recommendations for",userid,"\n",record)
        

    #print(recommendations)
    recommendedProductsDf = pd.DataFrame(recommendations, columns=["userid","productid","score"])
    print(recommendedProductsDf.head(10))
    recommendedProductsDf.to_csv("../../../data/processed/als-recommendations.csv")
    #calculate_MAP(recommendedProductsDf,)
    # find related items
    #related = model.similar_items(itemid)

if __name__ == "__main__":
    start_time = time.time()
    main() 
    d = timedelta(seconds=(time.time()-start_time))
    print("\n--- total run time (): " , d,"\n")
