import pandas as pd
import json
import os
import csv
from io import StringIO

def _exportMinMax(filePath, content):
    df = pd.read_csv(StringIO(content), sep='\\s+', encoding = 'utf-8', header=None)
    df = df.transpose()
    df1 = df.ix[:,1].str.split('|', expand=True)
    res = pd.concat([df.iloc[:, 0:1], df1.iloc[:, 0:2]], axis=1)
    with open(filePath, "w") as fp:
        res.to_csv(fp, sep="\t", header=False, index=False)

def _exportPopulation(filePath, content):
    colNames = ['ColumnName', 'NEG', 'POS', 'TOTAL', 'POPULATED', 'NIL']
    df = pd.read_csv(StringIO(content), sep='\\s+', encoding = 'utf-8',
            )
    df = df.transpose()
    df.columns = df.loc['ColumnName']
    df = df.reindex(df.index.drop('ColumnName'))
    with open(filePath, "w") as fp:
        df.to_csv(fp, sep="\t", header=True, index=True,
            index_label='ColumnName', columns=colNames)

def main(inStr):
    inObj = json.loads(inStr)
    fc = inObj["fileContents"]
    filePath = inObj["filePath"]
    folderPath = os.path.dirname(filePath)
    tableName, statType, _ = folderPath.split("/")[-1].split("_")
    os.makedirs(os.path.dirname(filePath), exist_ok=True)

    if folderPath.endswith('_minmax_stats'):
        _exportMinMax(filePath, fc)
    elif folderPath.endswith('_population_stats'):
        _exportPopulation(filePath, fc)
    else:
        with open(filePath, "w") as fp:
            fp.write(fc)
