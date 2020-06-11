
import pandas as pd
import time
from datetime import timedelta


def main(): 
    """[Main Function]
    """
    #Load query prod_ids and Similarity Matrix
    inputBasketFilePath = "../../../data/queryProdIds.txt"
    with open(inputBasketFilePath,'r') as f:
        inputBasket = [int(l.strip()) for l in f]
    
    simMatfilePath = "../../../data/processed/concatfiles/allPriorOrdersProductsSims.txt"
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
    
    
#======================================================
# Method 3: using a dicts for products and reading only Top 3: Fastest function
def method3(simMatfilePath,inputBasket):
    similarProducts = []
    prodDict = {}
    prodFilePath = "../../../data/original/products.csv"

    with open(prodFilePath,mode='r') as f:
        next(f)
        for line in f:
            record = line.strip().split(',')
            prodDict.update({int(record[0]):str(record[1])})

    for prod_id in inputBasket:
        i=0
        file = open(simMatfilePath)
        for line in file:
            record = line.strip().split('|')
            if ((int(record[0]) == prod_id) & (i <= 3)):
                similarProducts.append(record)
                i+=1
            if i >= 4:
                file.close()
                break
                
    df = pd.DataFrame.from_records(similarProducts, columns=['product_id_left','product_id_right','cosine_sims'])
    
    df = df.astype({'product_id_left': 'int64','product_id_right': 'int64','cosine_sims': 'float64'})
    
    df['product_name_left'] = df['product_id_left'].map(prodDict)
    df['product_name_right'] = df['product_id_right'].map(prodDict)

    for prod_id in inputBasket:
        print("Top 3 Similar Items to: ", prod_id, "\n",df[df['product_id_left']== prod_id].sort_values('cosine_sims',ascending=False)[1:])
        
   
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
    print("--- total run time (): " , d)

