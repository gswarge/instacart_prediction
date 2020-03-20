#import numpy as np
from data import data as d
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.plotly as py
import plotly.graph_objs as go
from pyspark.sql.functions import col
from pyspark.sql.types import *


def setplotStyle(ax, title, xlabel, ylabel):
      sns.set_style('ticks')
      sns.despine()
      sns.cubehelix_palette(20, start=.5, rot=-.75)
      plt.title(title, fontsize = 18)
      plt.xlabel(xlabel, fontsize = 12)
      plt.ylabel(ylabel, fontsize = 12)
      loc, labels = plt.xticks()
      ax.set_xticklabels(labels,rotation=45)

      
def main():
    """ 
    Main Function
    """
    ordersdf, merged_productsdf = d.loadData()
    
    print("Merged products, departments and aisles dataframe.")
    print("There are total {} orders in the datset\n".format(ordersdf.count()))
    
    #print("Orders groupby: user_id")
    #ordersdf.groupBy('user_id').count().sort(col("count").desc()).show()
    #merged_productsdf.printSchema()

    print("Products in each departments: \n")
    merged_productsdf.groupBy('department').count().sort(col("count").desc()).show()
    print("Products in top 20 aisles:\n")
    merged_productsdf.groupBy('aisle').count().sort(col("count").desc()).show()
    
    prod_aisles = [item for item in merged_productsdf.groupBy('aisle').count().sort(col("count").desc()).collect()]
    prod_dept = [item for item in merged_productsdf.groupBy('department').count().sort(col("count").desc()).collect()]
    prod_aisles = pd.DataFrame(prod_aisles,columns=['aisle','prod_count'])
    prod_dept = pd.DataFrame(prod_dept, columns=['department','prod_count'])
    #print(prod_aisles)
    #print(prod_dept)
    #plt.plot(x=prod_aisles['aisle_id'][:10],y=prod_aisles['count'][:10],color='red',figsize=(15,10))
    #sns.distplot(productsdf['aisle_id'].collect(),bins=20)
    
    
    
    ax = sns.barplot(x=prod_aisles['aisle'][:20],y=prod_aisles['prod_count'][:20])
    setplotStyle(ax,"Top 20 Aisles","aisles","product count")
    plt.savefig('plots/aisles_distribution.png')
    plt.show()
    

    ax = sns.barplot(x=prod_dept['department'],y=prod_dept['prod_count'])
    setplotStyle(ax,"Departments","Departments", "product count")
    plt.savefig('plots/dept_distribution.png')
    plt.show()
    


if __name__ == "__main__":
    main()
