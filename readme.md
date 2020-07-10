_(This Project is still work in progress, please forgive the messy codebase)_
## Implicit Recommender System
In this project I'm trying to build a recommender system based on the dataset published by [Instacart](https://www.instacart.com/datasets/grocery-shopping-2017)
Dataset contains over **3 Million+** transaction records of over **200K+ users**, with around **50k Products**.

### Goal
- My goal was to create a implicit collaborative filtering based recommender system based on this transactional dataset. I ended up creating different models to generate recommendations. I used **Mean Average Precision** as my measure for my models recommendations. Here is the list of my goals for this project:
    - Start with randomly recommending products from top 60 most purchased products.
    - Move on to generating Item-similarity based recommender system using **cosine similarity** as a distance measure.(Cosine similarity is based on co-occurances of the of products purchased together)
    - Generate a Matrix factorisation based recommender using **Alternate Least Square** method.
    - Generate a Matrix factorisation based recommender using **Latent Semantic Analysis** method._(Still Pending)_
    - Use a **Approximate Nearest Neighbour** algorithm to speed up the search for recommendations. _(Still Pending)_
    - Figure out a way to deploy this as a webapp. _(Still Pending)_

### Steps Taken:
Broadly the steps I've taken so far are:
- Load and Process data: 
  - I was facing memory problems while processing the data in Python, so I used **Apache Spark** to perform initial data processing. 
  - Spark code, written in **Scala** imports the data and after processing it, outputs a co-occurance matrix of all the purchases for 200k users and calculates cosine similarities between all the products (based on co-occurances). It outputs the processed data in *.csv* format.
 - **Cosine Similarity based Model**: Using the generated simialrity matrix above, generate recommendations based on Users 2nd Last order
 - **ALS based Model**:decompose the cooccurance matrix above using ALS and generate recommendations
 - compare the recommendations to the last order and generate **MAP** score

### Result:
If you still need to use and run the code: You need to download data published by [Instacart, here.](https://www.instacart.com/datasets/grocery-shopping-2017)  and extract it in data folder for the import to work.
### Main files:
#### Scala Files:
- src/main/scala/InstacartMain.scala : main file for spark processes, acts as a pipeline 
- src/main/scala/etl/dataProcessing.Scala : as name suggests, Main function which processes all data files
#### Python Files:
- src/main/python/predictions.py : Does the heavy work of generating predictions for all models except ALS.
- src/main/python/als.py : Implementation of Alternate Least Squares Matrix Factorisation.

