import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm
from fuzzywuzzy import fuzz
import multiprocessing as mp

Cores = mp.cpu_count()

# Module 1.0 Importing and Structuring Data
ACTSData = pd.read_csv("ACTS Masterlist.csv", encoding='latin1')
ACTSdf = pd.DataFrame(ACTSData, columns=['Name', 'CG', 'PersonalID', 'ID'])
ACTSNameCG = ACTSdf["Name"] + ' ' + ACTSdf["CG"]

ExceptionTable = []
for i in range(len(ACTSNameCG)):
    ExceptionTable.append([])

ActsStructured = []
for i in range(len(ACTSNameCG)):
    ActsStructured.append([])

for j in range(len(ACTSNameCG)):
    ActsStructured[j].append(ACTSdf["ID"][j])
    ActsStructured[j].append(ACTSdf["PersonalID"][j])
    ActsStructured[j].append(ACTSdf["Name"][j])
    ActsStructured[j].append(ACTSdf["CG"][j])

MinistryData = pd.read_csv("Ministry Masterlist.csv", encoding='latin1')
Ministrydf = pd.DataFrame(MinistryData, columns=['Name', 'CG', 'Ministry', 'IC'])
MinistryNameCG = Ministrydf["Name"] + ' ' + Ministrydf["CG"]

Tolerance = int(input("Enter Tolerance value from 0-100: "))
print(type(Tolerance))

# Module 1.1 Define Token Sort Function
def Comparator(c):
    DistTable = [fuzz.token_sort_ratio(MinistryNameCG[c], ACTSNameCG[i]) for i in range(len(ACTSNameCG))]
    return DistTable.index(max(DistTable)), max(DistTable)

# Module 1.2 Define Appender
def Appender(c):
    ind, para = Comparator(c)
    #print(para)
    if para > Tolerance:
        ActsStructured[ind].append(Ministrydf["Ministry"][c])
    else:
        ExceptionTable[c].append(Ministrydf["Name"][c])
        ExceptionTable[c].append(Ministrydf["CG"][c])
        ExceptionTable[c].append(Ministrydf["Ministry"][c])
        ExceptionTable[c].append(para)
        print(c,para)
    return


# Module 1.3 Iterator
Parallel(n_jobs=Cores, require='sharedmem')(delayed(Appender)(n) for n in tqdm(range(len(MinistryNameCG))))

# Module 1.4 - Export Function
ActsExport = pd.DataFrame(ActsStructured)
ActsExport.to_csv('ACTS Export.csv')
ExceptionExport = pd.DataFrame(ExceptionTable)
ExceptionExport.to_csv('Exception Export.csv')
