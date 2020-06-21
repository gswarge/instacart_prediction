import pandas as pd
import time
import json
from datetime import timedelta


def main(): 
    mapdict = []
    mapscorepath = "../../../data/processed/MAPScores.txt"
    

    file = open(mapscorepath,'r')
    MAPscores = file.read()
    MAPscores = MAPscores.replace('{','')
    MAPscores = MAPscores.replace('}','')
    MAPscores = MAPscores.replace('"','')
    MAPscores = MAPscores.replace('\'','')
    MAPscores = MAPscores.split(',')
    for line in MAPscores:
        record = line.split(':')
        mapdict.append(record)
    
    mapdf = pd.DataFrame.from_records(mapdict,columns=["userid","MAP"])
    #mapdf = pd.DataFrame.from_dict(mapdict, orient="index",columns=["userid","MAP"]) 
    print(mapdf.head(15))
    filename = "../../../data/processed/MAPScores.csv"
    mapdf.to_csv(filename)

        
    #MAPscores = file.read()
    #MAPscores = MAPscores.replace('{','')
    #MAPscores = MAPscores.replace('}','')
    #MAPscores = MAPscores.replace(',',',\n')
    #print(MAPscores)
    #userid = MAPscores.split(',')
    #print(data)
    #data = json.loads(file.read())
    #mapdf = pd.DataFrame.from_dict(MAPscores, orient="index",columns=["userid","MAPScores"])
    #mapdf = pd.read_json(MAPscores,orient="split")
    #print(mapdf) 
    
    #print(mapdf) 
    
    #mapdf = pd.read_json(mapscores,orient="records")
    #with open(mapscores) as file:
    #    mapdf = pd.DataFrame.from_dict(file, orient="index")
    #print(mapdf.head(15))

if __name__ == "__main__":
    start_time = time.time()
    main()
    d = timedelta(seconds=(time.time()-start_time))
    print("--- total run time (): " , d)

