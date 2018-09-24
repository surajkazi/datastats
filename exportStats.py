import pandas as pd
import json
import os
import csv
from io import StringIO

def _exportTransposeRes(filePath, content, cols, na_rep=''):
    df = pd.read_csv(StringIO(content), sep=',', encoding = 'utf-8')
    df = df.transpose()
    df = df[0].str.split('|', expand=True)
    df.columns = df.loc[cols[0]]
    df = df.reindex(df.index.drop(cols[0]))
    with open(filePath, "w") as fp:
        df.to_csv(fp, sep=",", na_rep=na_rep, header=True, index=True,
            index_label=cols[0], columns=cols[1:],
            quotechar = "\"", quoting=csv.QUOTE_MINIMAL)

def main(inStr):
    inObj = json.loads(inStr)
    fc = inObj["fileContents"]
    filePath = inObj["filePath"]
    folderPath = os.path.dirname(filePath)
    tableName, statType, _ = folderPath.split("/")[-1].split("_")
    os.makedirs(os.path.dirname(filePath), exist_ok=True)

    if folderPath.endswith('_minmax_stats'):
        colNames = ['ColumnName', 'Min', 'Max']
        _exportTransposeRes(filePath, fc, colNames)
    elif folderPath.endswith('_population_stats'):
        colNames = ['ColumnName', 'TOTAL', 'NEG', 'POS', 'ZERO', 'POPULATED',
                'NIL', 'SPACE']
        _exportTransposeRes(filePath, fc, colNames, na_rep=0)
    else:
        with open(filePath, "w") as fp:
            fp.write(fc)
