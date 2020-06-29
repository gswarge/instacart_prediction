This Project is still work in progress, please forgive the messy codebase.
If you still need to use and run the code: You need to download data from: https://www.instacart.com/datasets/grocery-shopping-2017 and extract it in data folder for the import to work.
Spark code, written in Scala, imports the data and after processing it, outputs a co-occurance matrix and cosine similarity matrix between all the products(based on whether they were purchased together)
Python files generates the similarites.
 - Currently it a simple item similarity based recommender. Im working on including Matrix factorisation techniques.

