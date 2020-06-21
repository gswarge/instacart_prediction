
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
import time
import csv
from datetime import timedelta


def main():
    pipeline()

def pipeline(): 
    """[Main Function]
    """
    #Load query prod_ids and Similarity Matrix
    #inputBasketFilePath = "../../../data/queryProdIds.txt"
    #with open(inputBasketFilePath,'r') as f:
    #    inputBasket = [int(l.strip()) for l in f]

    lastPriorOrderFilePath  = "../../../data/processed/concatfiles/lastPriorOrder.csv"
    simMatfilePath = "../../../data/processed/concatfiles/allPriorOrdersProductsSims.txt"
    prodFilePath = "../../../data/original/products.csv"
    allTrainOrdersFilePath = "../../../data/processed/concatfiles/allTrainOrders.csv"
    noOfUsers = 1000
    

    #=========================================================================
    # extract the Baskets for predictions | Extract for particular user(s)
    extractBaskets_time = time.time()
    inputBasket = extractBaskets(lastPriorOrderFilePath, allTrainOrdersFilePath,noOfUsers)
    d1 = timedelta(seconds=(time.time()-extractBaskets_time))
    #print("\n\n--- extractBaskets run time : ",d1,"\n")
    
    #=========================================================================
    # Method generatePreds: generate similar items for the input basket provided
    method3_time = time.time()
    df,inputBasket,actualTrainBasket = generatePreds(simMatfilePath,inputBasket,prodFilePath,allTrainOrdersFilePath)
    d2 = timedelta(seconds=(time.time()-method3_time))
    #print("\n\n--- generatePreds run time: ",d2,"\n")
    
    #=========================================================================
    # calculate Mean average precision for each user in our input Basket List
    calculate_MAP(df,inputBasket,actualTrainBasket)

def extractBaskets(lastPriorOrderFilePath, allTrainOrdersFilePath,noOfUsers):
    print("\n\nExtracting Product baskets for second last orders \n(i.e last order from prior orders dataset)... \n")
    
    #extracting product_ids of last order for each user
    lastPriorOrder = pd.read_csv(lastPriorOrderFilePath, usecols=['user_id','product_id'])
    allTrainOrders = pd.read_csv(allTrainOrdersFilePath, usecols=['user_id','product_id'])

    #=========================================================================
    #Removing Header rows, since i concatnated csv files from Spark export
    #print("prior order size before dropping header rows", lastPriorOrder.shape)
    headerRows = lastPriorOrder[lastPriorOrder['product_id'] == "product_id"]
    lastPriorOrder = lastPriorOrder.drop(headerRows.index, axis=0)
    #print("prior order size after dropping header rows", lastPriorOrder.shape)
    
    #print("train order size before dropping header rows", allTrainOrders.shape)
    headerRows = allTrainOrders[allTrainOrders['product_id'] == "product_id"]
    allTrainOrders = allTrainOrders.drop(headerRows.index, axis=0)
    #print("train order size after dropping header rows", allTrainOrders.shape)

    #=========================================================================
    #extracting basket of 10 random users, while ensuring only pick users who are in Train dataset
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
def generatePreds(simMatfilePath,inputBasket, prodFilePath,allTrainOrdersFilePath):
    print("\n*** Generating Predictions ***\n\n")
    
    #inputBasket = inputBasket.astype({'product_id': 'int64','user_id': 'int64'})
    basketList = inputBasket.values
    inputBasketUserList = inputBasket['user_id'].unique()
    
    #=========================================================================
    #Removing Header rows, since i concatnated csv files from Spark export
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
            if ((int(record[0]) == int(prod_id)) & (i <= 2) & (float(record[2]) < 0.999)):
                record.append(user_id)
                similarProducts.append(record)
                i+=1
            if i >= 3:
                file.close()
                j+=1
                break
                
    df = pd.DataFrame.from_records(similarProducts, columns=['purchased_product','predicted_product','cosine_sims','user_id'])
    
    df = df.astype({'purchased_product': 'int64','predicted_product': 'int64','cosine_sims': 'float64','user_id':'int64'})
    #=========================================================================
    # Creating a dict of product_id and product_name for faster Product_name lookup
    prodDict = {}
    
    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})

    df['prior_product_name'] = df['purchased_product'].map(prodDict)
    df['predicted_product_name'] = df['predicted_product'].map(prodDict)

    print("\n\nPredictions Generated....")
    
    return df,inputBasket,actualTrainBasket

def calculate_MAP(df,inputBasket,actualTrainBasket):
    #=========================================================================
    # Calculating mean average precision for each user
    print("\n*** Calculating MAP For each user ***\n")
    userList = inputBasket['user_id'].unique()
    print ("\nuserList: ",userList)
    map = {}

    for userid in userList:
        predicted = df[df['user_id'] == int(userid)]
        actual = actualTrainBasket[actualTrainBasket['user_id'] == int(userid)]
        actualBoughtList = actual['product_id'].values
        predictedBoughtList = predicted['predicted_product'].values
        #print("Input Boughtlist length:",len(actualBoughtList))
        #print("Predicted bought length:",len(predictedBoughtList))
        map.update({userid:(mean_avg_precision(actualBoughtList,predictedBoughtList,len(predictedBoughtList)))})

        print("\nuserid:",userid," | MAP: ",map.get(userid))

    #=========================================================================
    #Saving our predictions
    filename = "../../../data/processed/samplePredictions.csv"
    print("\n\nSaving predictions at: ",filename)
    df.to_csv(filename,index=False)
    filename = "../../../data/processed/MAEScores.txt"
    print("Saving MAP scores at: ",filename)
    print(map, file=open(filename, 'w'))
    print("\n\nPredictions saved...")
       
        
def mean_avg_precision(actual, predicted, k=3):
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
    main()
    d = timedelta(seconds=(time.time()-start_time))
    print("\n--- total run time (): " , d,"\n")

