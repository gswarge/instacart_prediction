import pandas as pd
import time
from datetime import timedelta


def main(): 
    """[Main Function]
    to generate similar products for users in last_orders set so that we can compare it with their last orders
    To begin with im only going to look at the last order from their prior_orders
    """
    filePath = "../../../data/processed/concatfiles/allPriorOrderProductsUsers.csv"
    lastOrdersDf = pd.read_csv(filePath, sep=",")

    lastOrdersDf.head(5)
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    d = timedelta(seconds=(time.time()-start_time))
    print("--- total run time (): " , d)

