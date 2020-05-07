import numpy as np
import data as d
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col
from pyspark.sql.types import *


def main(): 
    """[Main Function]
    """
    filteredDfCooccurances = "../../../data/filteredDfCooccurances.csv"
    inputDf = d.loadCSV(filteredDfCooccurances)
    inputDf.show(10)


if __name__ == "__main__":
    main()
