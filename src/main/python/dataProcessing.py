import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt 
import scipy.stats
import time
from datetime import timedelta



def load_orders():
    csvPath = {"orders":"../../../data/orders.csv","aisles":"../../../data/aisles.csv","departments":"../../../data/departments.csv","products":"../../../data/products.csv","opp":"../../../data/order_products_prior.csv","opt":"../../../data/order_products_train.csv"}
    def convert_eval_set(value):
        if 'prior' == value:
            return np.int8(1)
        elif 'train' == value:
            return np.uint8(2)
        else:
            return np.uint8(3) # 'test'
    
    def convert_days_since_prior_order(value):
        # 30 is the maximum value
        if '' == value:
            return np.int8(-1)
        else:
            return np.int8(np.float(value))

    orders = pd.read_csv(csvPath.get("orders"),
                            dtype={'order_id':np.uint32,
                            'user_id':np.uint32,
                            'order_number':np.uint8,
                            'order_dow':np.uint8,
                            'order_hour_of_day':np.uint8},
                            converters={'eval_set':convert_eval_set,
                                'days_since_prior_order':convert_days_since_prior_order})
    orders = orders.astype({'eval_set':np.uint8,'days_since_prior_order':np.int8})

    return orders

def load_orders_prods(path):
    return pd.read_csv(path, dtype={'order_id': np.uint32,
                                    'product_id': np.uint32,
                                    'add_to_cart_order': np.uint8,
                                    'reordered': np.uint8})

def load_products(path):
    return pd.read_csv(path, dtype={'product_id': np.uint16,
                                    'aisle_id': np.uint8,
                                    'department_id': np.uint8})

def load_aisles(path):
    return pd.read_csv(path, dtype={'aisle_id': np.uint8})

def load_depts(path):
    return pd.read_csv(path, dtype={'department_id': np.uint8})


def main():

    pass

if __name__ == "__main__":
    start_time = time.time()
    main()
    d = timedelta(seconds=(time.time()-start_time))
    print("--- total run time (): " , d)    