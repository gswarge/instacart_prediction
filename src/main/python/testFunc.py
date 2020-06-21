import pandas as pd
import time
from datetime import timedelta


def main(): 
    priorOrders = pd.read_csv("../../../data/processed/concatfiles/")
    trainOrders = pd.read_csv("../../../data/processed/concatfiles/")
    testOrders = pd.read_csv("../../../data/processed/concatfiles/")
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    d = timedelta(seconds=(time.time()-start_time))
    print("--- total run time (): " , d)

