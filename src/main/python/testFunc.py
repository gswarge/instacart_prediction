import pandas as pd
import time
from datetime import timedelta
import argparse
import glob, os


def main(mapfolderpath): 
    #This function reads all MAP scores via different models and finds a global mean of that model
    print("\nGenerating global MAP scores for all models\n")
    all_files = glob.glob(os.path.join(mapfolderpath, "map-*.csv"))
    globalMAP = []
    for f in all_files:
        df = pd.read_csv(f)
        meanMAP = df['MAP'].mean()
        modelname = f[f.find(mapfolderpath)+len(mapfolderpath):f.rfind(".csv")]
        record= [modelname,meanMAP]
        globalMAP.append(record)
        print (modelname,meanMAP,sep=" : ")
        #print(f)
    #print("\n",globalMAP)

   
def txttocsv():
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
    print(mapdf.head(15))
    filename = "../../../data/processed/MAPScores.csv"
    mapdf.to_csv(filename)

if __name__ == "__main__":
    start_time = time.time()
    parser = argparse.ArgumentParser(description="Generates Global MAP for all models",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--mapfolderpath', type=str, default='../../../data/processed/',
                        dest='mapfolderpath', help='folder path for MAP scores, file name format map-[modelname].csv')
    args = parser.parse_args()   
    main(args.mapfolderpath)
    run_time = timedelta(seconds=(time.time()-start_time))
    print("\n--- total run time (): " , run_time)

