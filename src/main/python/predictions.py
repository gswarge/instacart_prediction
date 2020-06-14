
import pandas as pd
import time
from datetime import timedelta
from sklearn.metrics import average_precision_score

def main(): 
    """[Main Function]
    """
    #Load query prod_ids and Similarity Matrix
    #inputBasketFilePath = "../../../data/queryProdIds.txt"
    #with open(inputBasketFilePath,'r') as f:
    #    inputBasket = [int(l.strip()) for l in f]

    last2OrdersFilePath  = "/Users/apple/MEGA/Personal/My_Projects/DS_Projects/instacart_prediction/data/processed/concatfiles/last2UserOrders.csv"
    simMatfilePath = "/Users/apple/MEGA/Personal/My_Projects/DS_Projects/instacart_prediction/data/processed/concatfiles/allPriorOrdersProductsSims.txt"
    prodFilePath = "/Users/apple/MEGA/Personal/My_Projects/DS_Projects/instacart_prediction/data/original/products.csv"
    

    #======================================================
    # extract the Baskets for predictions | Extract for particular user(s)
    extractBaskets_time = time.time()
    inputBasket,lastOrders = extractBaskets(last2OrdersFilePath)
    d = timedelta(seconds=(time.time()-extractBaskets_time))
    print("\n--- extractBaskets run time : ",d)
    
    #======================================================
    # Method generatePreds: generate similar items for the input basket provided
    method3_time = time.time()
    generatePreds(simMatfilePath,inputBasket,prodFilePath)
    d = timedelta(seconds=(time.time()-method3_time))
    print("--- generatePreds run time: ",d)
    

def extractBaskets(last2OrdersFilePath):
    print("\nExtracting Product baskets for second last orders \n(i.e last order from prior orders dataset)... \n\n")
    
    #extracting product_ids of last order for each user
    lastOrders = pd.read_csv(last2OrdersFilePath)

    #there are some rows with values as "product_id", taking those out
    emptyRows = lastOrders[lastOrders['product_id'] == "product_id"]
    lastOrders = lastOrders.drop(emptyRows.index)
    
    #Dropping columns which arent needed for now
    lastOrders.drop(["order_id","order_number","2ndLastOrder","lastOrder"],axis=1,inplace=True)
    
    #extracting basket of 5 random users
    userList = lastOrders['user_id'].sample(5,random_state=1)
    inputBasket = lastOrders[lastOrders['user_id'].isin(userList)]
    print("Sample Input basket: \n",inputBasket)
    return inputBasket,lastOrders
    


#======================================================
# Method generatePreds: using a dicts for products and reading only Top 3: Fastest function
def generatePreds(simMatfilePath,inputBasket, prodFilePath):
    print("\n\n**** generating predictions****\n\n")
    similarProducts = []
    prodDict = {}
    inputBasket = inputBasket.astype({'product_id': 'int64','user_id': 'int64'})
    prodList = inputBasket['product_id'].values
    #userList = inputBasket['user_id'].values
    #prodList = pd.unique(prodList)
    print("basketsize:",prodList.shape )
    #return
    
    #looking up top 3 similar items for each product in the input basket
    j=0
    for prod_id in prodList:
        i=0
        print(j,prod_id,sep=":",end=" | ",flush=True)
        file = open(simMatfilePath)
        for line in file:
            record = line.strip().split('|')
            if ((int(record[0]) == int(prod_id)) & (i <= 2) & (float(record[2]) < 0.999)):
                #print("similar_product",record[1],end=",",sep=":")
                
                similarProducts.append(record)
                i+=1
            if i >= 3:
                file.close()
                j+=1
                break
                
    df = pd.DataFrame.from_records(similarProducts, columns=['purchased_product','similar_product','cosine_sims'])
    
    df = df.astype({'purchased_product': 'int64','similar_product': 'int64','cosine_sims': 'float64'})
    
    # Creating a dict of product_id and product_name for faster Product_name lookup
    print("\ngenerating prediction for each Product_id:")
    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})

    df['product_name'] = df['purchased_product'].map(prodDict)
    df['similar_product_name'] = df['similar_product'].map(prodDict)
    print(df.head(10))
    finalDf = df.merge(inputBasket,right_on="product_id",left_on="purchased_product")
    
    print("\n\nPredictions Generated....")
    print(finalDf.head(25))
    
    #for prod_id in inputBasket:
    #    print("Top 3 Similar Items to: ", prod_id, "\n",df[df['product_id']== prod_id].sort_values('cosine_sims',ascending=False)[1:])
    
    finalDf.to_csv("../../../data/processed/samplePredictions.csv",index=False)
    print("\n\npredictions written to a csv file... ")
    return df
       
        
   
#====================================================== 
# Method 1 

def method1(filePath,queryList): 
    records = []
    productsdf = pd.read_csv('../../../data/original/products.csv')
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


#======================================================
# Method 2
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
    print("\n--- total run time (): " , d,"\n")

