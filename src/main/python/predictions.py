
import pandas as pd
import time
from datetime import timedelta


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
    # extract the Baskets for predictions
    extractBaskets_time = time.time()
    inputBaskets,lastOrders = extractBaskets(last2OrdersFilePath)
    d = timedelta(seconds=(time.time()-extractBaskets_time))
    print("\n--- extractBaskets run time : ",d)
    
    #======================================================
    # Method 3: using a dicts for products
    method3_time = time.time()
    generatePreds(simMatfilePath,lastOrders,prodFilePath)
    d = timedelta(seconds=(time.time()-method3_time))
    print("--- method 3 run time: ",d)
    

def extractBaskets(last2OrdersFilePath):
    print("\nExtracting Product baskets for second last orders (i.e last order from prior orders dataset)... \n")
    
    print(last2OrdersFilePath)
    #extracting product_ids of last order for each user
    
    lastOrders = pd.read_csv(last2OrdersFilePath)

    #there are some rows with values as "product_id", taking those out
    emptyRows = lastOrders[lastOrders['product_id'] == "product_id"]
    print(emptyRows)
    lastOrders = lastOrders.drop(emptyRows.index)
    
    #Dropping columns which arent needed for now
    lastOrders.drop(["order_id","order_number","2ndLastOrder","lastOrder"],axis=1,inplace=True)
    print(lastOrders.head(10))
    inputBasket = lastOrders['product_id']
    
    print("\n\nbasket extracted..")
    return inputBasket,lastOrders
    



#======================================================
# Method generatePreds: using a dicts for products and reading only Top 3: Fastest function
def generatePreds(simMatfilePath,inputBasket, prodFilePath):
    print("\n\n**** generating predictions****")
    similarProducts = []
    prodDict = {}

    #inputBasket['product_id'] = inputBasket['product_id'].astype('int64')
    
    prodList = inputBasket['product_id'].values
    #userList = inputBasket.user_id.unique()
    prodList = pd.unique(prodList)
    
    #looking up top 3 similar items for each product in the input basket
    
    j=0
    for prod_id in prodList:
        i=0
        print(j,prod_id,sep=":",end=" | ",flush=True)
        file = open(simMatfilePath)
        for line in file:
            record = line.strip().split('|')
            if ((int(record[0]) == int(prod_id)) & (i <= 2)):
                #print("similar_product",record[1],end=",",sep=":")
                similarProducts.append(record)
                i+=1
            if i >= 3:
                file.close()
                j+=1
                break
                
    df = pd.DataFrame.from_records(similarProducts, columns=['product_id_left','product_id_right','cosine_sims'])
    
    df = df.astype({'product_id_left': 'int64','product_id_right': 'int64','cosine_sims': 'float64'})
    
    # Creating a dict of product_id and product_name for faster Product_name lookup
    print("\ngenerating prediction for each Product_id:")
    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})

    df['product_name_left'] = df['product_id_left'].map(prodDict)
    df['product_name_right'] = df['product_id_right'].map(prodDict)

    #for prod_id in inputBasket:
    #    print("Top 3 Similar Items to: ", prod_id, "\n",df[df#['product_id_left']== prod_id].sort_values('cosine_sims',ascending=False)[1:])
    print("\n\nPredictions Generated....")
    df.to_csv("../../../data/processed/generatedPredictions.csv")
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
    print("\n\n--- total run time (): " , d)

