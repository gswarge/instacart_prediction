#import numpy as np
import data as d
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.plotly as py
import plotly.graph_objs as go
from pyspark.sql.functions import col
from pyspark.sql.types import *


def setplotStyle(ax, title, xlabel, ylabel):
      plt.style.use('fivethirtyeight')
      loc, labels = plt.xticks()
      ax.set_xlabel(xlabel,fontsize=15,color='orange')
      ax.set_ylabel(ylabel,fontsize=15,color='blue')
      ax.set_title(title, fontsize=18)
      ax.set_xticklabels(labels,rotation=45)
      sns.despine(ax=ax)
      
      

def prod_groupby(merged_productsdf):
    print("Products in each departments: \n")
    merged_productsdf.groupBy('department').count().sort(col("count").desc()).show()
    print("Products in top 20 aisles:\n")
    merged_productsdf.groupBy('aisle').count().sort(col("count").desc()).show()
    
    prod_aisles = [item for item in merged_productsdf.groupBy('aisle').count().sort(col("count").desc()).collect()]
    prod_dept = [item for item in merged_productsdf.groupBy('department').count().sort(col("count").desc()).collect()]
    prod_aisles = pd.DataFrame(prod_aisles,columns=['aisle','prod_count'])
    prod_dept = pd.DataFrame(prod_dept, columns=['department','prod_count'])
    
    #plt.plot(x=prod_aisles['aisle_id'][:10],y=prod_aisles['count'][:10],color='red',figsize=(15,10))
    #sns.distplot(productsdf['aisle_id'].collect(),bins=20)

    plt.figure(figsize=[25,15])
    ax = sns.barplot(x=prod_aisles['aisle'][:20],y=prod_aisles['prod_count'][:20])
    setplotStyle(ax,"Top 20 Aisles","aisles","product count")
    plt.savefig('plots/aisles_distribution.png')
    #plt.show()
    plt.figure(figsize=[30,15])
    ax = sns.barplot(x=prod_dept['department'],y=prod_dept['prod_count'])
    setplotStyle(ax,"Departments","Departments", "product count")
    plt.savefig('plots/dept_distribution.png')
    #plt.show()     

def analyse_id(merged_productsdf):
    print(merged_productsdf.show())





def main():
    """ 
    Main Function
    """
    ordersdf, merged_productsdf = d.loadData()
    print("\nThere are total {} orders in the datset\n".format(ordersdf.count()))

    #analysing Id's column
    analyse_id(merged_productsdf)

    #Plotting bar chart to check product distribution in aisles and departments
    #prod_groupby(merged_productsdf)
    

if __name__ == "__main__":
    main()
