
import argparse
import pandas as pd
import numpy as np
import logging
import time
import csv
from datetime import timedelta
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def main(model_name,mapsavepath,noOfUsers,k): 
    """[Main Function]
    """
    basefolderpath = "../../../data/processed/"
    lastPriorOrderFilePath  = basefolderpath+"concatfiles/lastPriorOrder.csv"
    simMatfilePath = basefolderpath+"concatfiles/allPriorOrdersProductsSims.txt"
    prodFilePath = "../../../data/original/products.csv"
    allTrainOrdersFilePath = basefolderpath+"concatfiles/allTrainOrders.csv"
    

    print ("\n *** Running Predictions for",noOfUsers,"users, Using",model_name," model for k",k,"***", sep=": ")
    #=========================================================================
    # extract the Baskets for predictions | Extract for particular no of user(s)
    extractBaskets_time = time.time()
    inputBasket = extractBaskets(lastPriorOrderFilePath, allTrainOrdersFilePath,noOfUsers)
    d1 = timedelta(seconds=(time.time()-extractBaskets_time))
    #print("\n\n--- extractBaskets run time : ",d1,"\n")
    
    #=========================================================================
    # Method generatePreds: generate similar items for the input basket provided based on cosine similarity
    if model_name == "cosine":
        method3_time = time.time()
        mapsavepath = "map-cosine.csv"
        predictedBasketDf,actualTrainBasket = generatePredsCosine(simMatfilePath,inputBasket,prodFilePath,allTrainOrdersFilePath,k,basefolderpath)
        d2 = timedelta(seconds=(time.time()-method3_time))
        #print("\n\n--- generatePreds run time: ",d2,"\n")
    elif model_name == "random":
        mapsavepath = "map-random.csv"
        predictedBasketDf,actualTrainBasket = randomModel(inputBasket,prodFilePath,allTrainOrdersFilePath,k,basefolderpath)
    
    elif model_name == "baseline":
        mapsavepath = "map-baseline.csv"
        actualTrainBasket = removeHeaderRows(allTrainOrdersFilePath)
        predictedBasketDf,actualTrainBasket = baselineModel(inputBasket,prodFilePath,actualTrainBasket,lastPriorOrderFilePath,k,basefolderpath)

    else:
        raise NotImplementedError("TODO: model %s" % model_name)
        
    #=========================================================================
    # calculate Mean average precision for each user in our input Basket List
    calculate_MAP(predictedBasketDf,inputBasket,actualTrainBasket,mapsavepath,k)

def removeHeaderRows(filePath):
    #=========================================================================
    #Removing Header rows, since I concatnated csv files from Spark export, header rows for each files are also concatenated
    df = pd.read_csv(filePath,usecols=['user_id','product_id'])
    headerRows = df[df['product_id'] == "product_id"]
    df = df.drop(headerRows.index, axis=0)
    df = df.astype({'product_id': 'int64','user_id': 'int64'})
    return df

def extractBaskets(lastPriorOrderFilePath, allTrainOrdersFilePath,noOfUsers):
    print("\n\nExtracting Product baskets for second last orders \n(i.e last order from prior orders dataset)... \n")
    
    #extracting product_ids of last order for each user
    #lastPriorOrder = pd.read_csv(lastPriorOrderFilePath, usecols=['user_id','product_id'])
    #allTrainOrders = pd.read_csv(allTrainOrdersFilePath, usecols=['user_id','product_id'])

    #=========================================================================
    #Reading dataframe and Removing Header rows, since I concatnated csv files from Spark export
    lastPriorOrder = removeHeaderRows(lastPriorOrderFilePath)
    allTrainOrders = removeHeaderRows(allTrainOrdersFilePath)

    #=========================================================================
    #extracting basket of random number of users, while ensuring we only pick users who are in the Train dataset
    allTrainUsers = allTrainOrders['user_id'].unique()
    print("prior order size before filtering train users", lastPriorOrder.shape)
    lastPriorOrder = lastPriorOrder[lastPriorOrder['user_id'].isin(allTrainUsers)]
    print("prior order size after filtering train users", lastPriorOrder.shape)

    randomUserList = lastPriorOrder['user_id'].sample(noOfUsers,random_state=1)
    inputBasket = lastPriorOrder[lastPriorOrder['user_id'].isin(randomUserList)]
    
    print("\n\nSample inputBasket Extracted, basket size:", inputBasket.shape)

    return inputBasket
    


#=========================================================================
# Method generatePreds: using a dicts for products and reading only Top 3 similar items
def generatePredsCosine(simMatfilePath,inputBasket, prodFilePath,allTrainOrdersFilePath,k,basefolderpath):
    print("\n*** Generating predictions based on Cosine Similarity ***\n\n")
    
    #inputBasket = inputBasket.astype({'product_id': 'int64','user_id': 'int64'})
    basketList = inputBasket.values
    inputBasketUserList = inputBasket['user_id'].unique()
    
    #=========================================================================
    #Removing Header rows, since I concatnated csv files from Spark export
    actualTrainBasket = pd.read_csv(allTrainOrdersFilePath,usecols=['user_id','product_id'])
    headerRows = actualTrainBasket[actualTrainBasket['product_id'] == "product_id"]
    actualTrainBasket = actualTrainBasket.drop(headerRows.index, axis=0)
    #print("dropped header rows")
    actualTrainBasket = actualTrainBasket.astype({'product_id': 'int64','user_id': 'int64'})
    
    #=========================================================================
    #Selecting last (train) orders of users who are in our InputBasket
    print("Train basket size before filtering input users", actualTrainBasket.shape)
    actualTrainBasket = actualTrainBasket[actualTrainBasket['user_id'].isin(inputBasketUserList) ]
    print("Train basket size after filtering input users", actualTrainBasket.shape,"\n")

    #=========================================================================
    #looking up top 3 similar items for each product in the input basket
    j=0
    similarProducts = []
    
    for user_id, prod_id in basketList:
        i=0
        print(j,sep=":",end=" | ",flush=True)
        file = open(simMatfilePath)
        for line in file:
            record = line.strip().split('|')
            if ((int(record[0]) == int(prod_id)) & (i <= k) & (float(record[2]) < 0.999)):
                record.append(user_id)
                similarProducts.append(record)
                i+=1
            if i >= k:
                file.close()
                j+=1
                break
                
    predictedBasketDf = pd.DataFrame.from_records(similarProducts, columns=['purchased_product','predicted_product','cosine_sims','user_id'])
    
    predictedBasketDf = predictedBasketDf.astype({'purchased_product': 'int64','predicted_product': 'int64','cosine_sims': 'float64','user_id':'int64'})
   
    predictedBasketDf = productLookup(prodFilePath,predictedBasketDf)
    #=========================================================================
    #Saving our predictions
    filename = "cosinePredictions.csv"
    print("\n\nSaving predictions at: ",basefolderpath+filename)
    predictedBasketDf.to_csv(basefolderpath+filename,index=False)
    print("\n\nPredictions Saved....")
    
    return predictedBasketDf,actualTrainBasket

def productLookup(prodFilePath, predictedBasketDf):
    # Creating a dict of product_id and product_name for faster Product_name lookup
    prodDict = {}
    
    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})

    predictedBasketDf['prior_product_name'] = predictedBasketDf['purchased_product'].map(prodDict)
    predictedBasketDf['predicted_product_name'] = predictedBasketDf['predicted_product'].map(prodDict)

    return predictedBasketDf 

def randomModel(inputBasket,prodFilePath,allTrainOrdersFilePath,k,basefolderpath):
    print("\n*** Generating Predictions - random model ***\n\n")
    #This model basically randomly suggests a product from total products dataset
    inputBasket = inputBasket.astype({'product_id': 'int64','user_id': 'int64'})
    basketList = inputBasket.values
    inputBasketUserList = inputBasket['user_id'].unique()
    
    #=========================================================================
    #Removing Header rows, since i concatnated csv files from Spark export
    actualTrainBasket = removeHeaderRows(allTrainOrdersFilePath)
    actualTrainBasket = actualTrainBasket.astype({'product_id': 'int64','user_id': 'int64'})
    
    products = pd.read_csv(prodFilePath, usecols=["product_id"])

    predictedProducts = []
    j=0
    for j,userid,prod in enumerate(basketList):
        print(j,sep=":",end=" | ",flush=True)
        inputBasketprodList = inputBasket[inputBasket['user_id'] == int(userid)]['product_id'].values
        record=[]
        products['userid'] = userid
        products['purchased_product']=prod
        record = products.sample(k).values
        for item in range(0,len(record)):
            predictedProducts.append(record[item])
        
    predictedBasketDf = pd.DataFrame(predictedProducts, columns=["predicted_product","user_id","purchased_product"])
    predictedBasketDf = productLookup(prodFilePath,predictedBasketDf)
    #=========================================================================
    #Saving our predictions
    filename = "randomPredictions.csv"
    print("\n\nSaving predictions at: ",basefolderpath+filename)
    predictedBasketDf.to_csv(basefolderpath+filename,index=False)
    print("\n\nPredictions Saved....")

    return predictedBasketDf,actualTrainBasket

def baselineModel(inputBasket,prodFilePath,actualTrainBasket,lastPriorOrderFilePath,k,basefolderpath):

    #This model randomly suggests a product from top 100 most purchased products
    print("\n*** Generating Predictions - baseline model ***\n\n")
    
    inputBasket = inputBasket.astype({'product_id': 'int64','user_id': 'int64'})
    basketList = inputBasket.values
    inputBasketUserList = inputBasket['user_id'].unique()
    
    #=========================================================================
    #Selecting top purchased Items from orders history
    lastPriorOrder = pd.read_csv(lastPriorOrderFilePath, usecols=['user_id','product_id'])
    lastPriorOrder = removeHeaderRows(lastPriorOrderFilePath)
    topItems  = lastPriorOrder.groupby(['product_id']).agg(purchasecount=('product_id','count')).sort_values(by='purchasecount',ascending=False).reset_index().head(100)

    #=========================================================================
    #Selecting last (train) orders of users who are in our InputBasket
    print("Train basket size before filtering input users", actualTrainBasket.shape)
    actualTrainBasket = actualTrainBasket[actualTrainBasket['user_id'].isin(inputBasketUserList) ]
    print("Train basket size after filtering input users", actualTrainBasket.shape,"\n")
    
    predictedProducts = [];j=0
    for j,userid,prodid in enumerate(basketList):
        print(j,sep=":",end=" | ",flush=True)
        topItems['user_id'] = userid
        topItems['purchased_product']=prodid
        record = topItems.sample(k,random_state=1).values
        for item in range(0,len(record)):
            predictedProducts.append(record[item])
            
   
    predictedBasketDf = pd.DataFrame.from_records(predictedProducts, columns=["predicted_product","purchasecount","user_id","purchased_product"])
    predictedBasketDf = productLookup(prodFilePath,predictedBasketDf)
    #=========================================================================
    #Saving our predictions
    filename = "baselinePredictions.csv"
    print("\n\nSaving predictions at: ",basefolderpath+filename)
    predictedBasketDf.to_csv(basefolderpath+filename,index=False)
    print("\n\nPredictions Saved....")

    return predictedBasketDf,actualTrainBasket    



def calculate_MAP(predictedBasketDf,inputBasket,actualTrainBasket,mapsavepath,k):
    #=========================================================================
    # Calculating mean average precision for each user
    print("\n*** Calculating MAP For each user ***\n")
    
    userList = inputBasket['user_id'].unique()
    
    map = []
    
    for userid in userList:
        record =[]
        predicted = predictedBasketDf[predictedBasketDf['user_id'] == int(userid)]
        actual = actualTrainBasket[actualTrainBasket['user_id'] == int(userid)]
        actualBoughtList = actual['product_id'].values
        predictedBoughtList = predicted['predicted_product'].values
        record.append(userid)
        record.append(mean_avg_precision(actualBoughtList,predictedBoughtList,k))
        map.append(record)
        print("*",end="*")
        #print("userid",userid,sep=":", end=" | ")

    

    mapdf = pd.DataFrame.from_records(map,columns=['userid','MAP'])
    meanMap = mapdf['MAP'].mean()
    print("\n",mapdf.head(10))
    print("\nMean MAP : ",meanMap)
    filepath = "../../../data/processed/"
    print("\nSaving MAP scores at: ",filepath+mapsavepath)
    mapdf.to_csv(filepath+mapsavepath,index=False)
    print("\nPredictions saved...")
       
        
def mean_avg_precision(actual, predicted, k):
    if len(predicted) > k:
        predicted = predicted[:k]
    score = 0.0
    num_hits= 0.0

    for i,p in enumerate(predicted):
        if p in actual and p not in predicted[:i]:
            num_hits += 1.0
            score += num_hits / (i+1.0)
    
    #if not actual.any():
    #    return 0.0

    return np.mean(score / min(len(actual),k))


    
if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Generates related Items from the Instacart Dataset "
                                     "dataset: [Link]",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--mapsavepath', type=str, default='map-cosine.csv',
                        dest='mapsavepath', help='output file name for MAP scores')
    parser.add_argument('--model', type=str, default='cosine',
                        dest='model', help='model to calculate (als/cosine/random/baseline)')
    parser.add_argument('--noofusers', type=int, default=5,
                        dest='noofusers', help='calculate for no of users (als/cosine/null,topitems)')
    parser.add_argument('--k', type=int, default=10, dest='k',
                        help='How many items to predict per user?')
    parser.add_argument('--min_rating', type=float, default=4.0, dest='min_rating',
                        help='Minimum rating to assume that a rating is positive')
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    d = timedelta(seconds=(time.time()-start_time))
    main(args.model,args.mapsavepath,args.noofusers,args.k)
    print("\n--- total run time (): " , d,"\n")

