import argparse
import json
import hashlib
import time
import timeit
from datetime import datetime

from xcalar.compute.api.XcalarApi import XcalarApi
from xcalar.compute.api.Session import Session
from xcalar.compute.api.WorkItem import WorkItem
from xcalar.compute.api.ResultSet import ResultSet
from xcalar.compute.api.Operators import *
from xcalar.compute.api.Dataset import *
from xcalar.compute.api.WorkItem import *
from xcalar.compute.api.Udf import *
from xcalar.compute.api.Retina import *
from xcalar.compute.api.Target import Target
from xcalar.compute.api.Target2 import Target2
from xcalar.compute.coretypes.DagTypes.ttypes import *
from xcalar.compute.coretypes.UnionOpEnums.ttypes import *

here = os.path.abspath(os.path.dirname(__file__))

class DataStatsGenerator(object):
    def __init__(self, xcalarApi, args):
        self.xcApi = xcalarApi
        self.username = xcalarApi.session.username
        self.op = Operators(self.xcApi)
        self.udf = Udf(self.xcApi)
        self.retina = Retina(self.xcApi)
        self.exportTarget = Target(self.xcApi)
        self.importTarget = Target2(self.xcApi)

        self.importTargetName = args.importTargetName
        self.datasetPath = args.datasetPath
        self.scehmaPath = args.scehmaPath
        self.exportPath = args.exportPath
        self.createTargetsNUdfs()

    def createTargetsNUdfs(self):
        udfsFiles = ['stats.py', 'exportStats.py']
        for pyFile in udfsFiles:
            moduleName=pyFile.split(".")[0].lower()
            print ("Uploading %s" % (moduleName))
            with open(os.path.join(here, pyFile)) as fp:
                self.udf.addOrUpdate(moduleName, fp.read())

        #create export target
        self.exportTargetName = "export_stats_target"
        udfModule = "exportstats"
        sessionId = None
        for sess in self.xcApi.session.list().sessions:
            if sess.name == self.xcApi.session.name:
                sessionId = sess.sessionId
                break
        try:
            self.exportTarget.removeUDF(self.exportTargetName)
        except:
            pass
        try:
            exportUdfModule = "/workbook/{}/{}/udf/{}:main".\
                    format(self.username, sessionId, udfModule)
            self.exportTarget.addUDF(self.exportTargetName,
                                self.exportPath,
                                exportUdfModule)
        except Exception as e:
            print("Warning: Export target creation failed with:", str(e))
            raise e
        print("targets and UDFs created!")

    def loadDataset(self):
        print("Loading dataset..")
        tabsCreated = []
        timestamp = int(time.time())
        tableName = os.path.splitext(os.path.basename(self.datasetPath))[0]
        self.tableName = tableName
        self.getSchema()
        datasetName = "{}.{}.{}".format(self.username,
                    timestamp, tableName)
        args = {"linesToSkip":0, "schemaMode":"header"}
        if self.schema["filetype"] == "DELIMITED":
            args["fieldDelim"] = self.schema["delimiter"]
        else:
            print("Specifies file type {} not supported".format(
                    self.schema["filetype"]))
        args["fieldDelim"] = '\u2566'
        dataset = CsvDataset(self.xcApi, self.importTargetName,
            self.datasetPath, datasetName, **args)
        dataset.load()

        #index
        tabsCreated.append(self.getTableName("index"))
        self.op.indexDataset(dataset.name, tabsCreated[-1],
                "xcalarRecordNum", fatptrPrefixName=datasetName)

        #Map
        allCols = []
        evalStrs = []
        for col in self.schema["fields"]:
            colType = {"name":col['fieldname']}
            #TODO need to add numeric type
            if col["type"] == 'int':
                evalStrs.append("int({}::{})".format(datasetName, col['fieldname']))
                col["xType"] = "DfInt64"
            elif col["type"] == 'float' or col["type"] == 'decimal':
                evalStrs.append("int({}::{})".format(datasetName, col['fieldname']))
                col["xType"] = "DfFloat64"
            else:
                evalStrs.append("string({}::{})".format(datasetName, col['fieldname']))
                col["xType"] = "DfString"
            allCols.append(col['fieldname'])
        tabsCreated.append(self.getTableName("map"))
        self.op.map(tabsCreated[-2], tabsCreated[-1], evalStrs, allCols)

        #project
        tabsCreated.append(self.getTableName("project"))
        self.op.project(tabsCreated[-2], tabsCreated[-1], allCols)

        self.cleanUp(tabsCreated[:-1], [dataset])
        print("Done!")
        print("*"*60)
        return tabsCreated[-1]

    def getSchema(self):
        self.schema = None
        with open(self.scehmaPath) as f:
            self.schema = json.load(f)

    def getSplitNum(self, tab, col):
        tabsCreated = []
        delimiter = "|"

        #map
        tabsCreated.append(self.getTableName("splitnum_"+col))
        evalStr = "countChar({}, \"{}\")".format(col, delimiter)
        self.op.map(tab, tabsCreated[-1], evalStr, "numChars")

        #aggregate
        tabsCreated.append(self.getTableName("aggregate_"+col))
        evalStr = "max({})".format("numChars")
        self.op.aggregate(tabsCreated[-2], tabsCreated[-1], evalStr)
        resultSet = ResultSet(self.xcApi, tableName = tabsCreated[-1])
        maxNum = next(resultSet)['constant'];
        del resultSet

        self.cleanUp(tabsCreated)
        return maxNum + 1

    def genColPopStats(self, startTab, colName):
        tabsCreated = []
        #project
        tabsCreated.append(self.getTableName("project"+colName))
        self.op.project(startTab, tabsCreated[-1], [colName])

        #map
        tabsCreated.append(self.getTableName("map"+colName))
        evalStr = "stats:fieldPopStats({})".format(colName)
        tokensCol = colName+"_stats"
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                evalStr, tokensCol)

        statType = "StatType_"+colName
        # #explode string
        # tabsCreated.append(self.getTableName("explode"+colName))
        # evalStr = "explodeString({}, \"{}\")".format(colName+"_stats", "|")
        # self.op.map(tabsCreated[-2], tabsCreated[-1], evalStr, statType)

        # ##WORKAROUND FOR EXPLODE STRING
        maxNumSplits = self.getSplitNum(tabsCreated[-1], tokensCol)
        evalStrs = []
        newFields = []
        unionCols = []
        for x in range(1, maxNumSplits+1):
            evalStrs.append("cut({}, {}, \"|\")".format(tokensCol, x))
            newFields.append("{}_split{}".format(colName, x))
            unionCols.append([XcalarApiColumnT(newFields[-1],
                                statType, 'DfString')])

        #map
        tabsCreated.append(self.getTableName("splitCols"+colName))
        self.op.map(tabsCreated[-2], tabsCreated[-1], evalStrs, newFields)

        #union
        tabsCreated.append(self.getTableName("union"+colName))
        self.op.union([tabsCreated[-2]]*maxNumSplits, tabsCreated[-1],
                unionCols)

        #filter
        tabsCreated.append(self.getTableName("filter"+colName))
        filterStr = "neq({}, \"\")".format(statType)
        self.op.filter(tabsCreated[-2], tabsCreated[-1], filterStr)
        # ##WORKAROUND FOR EXPLODE STRING DONE

        #index for group by
        tabsCreated.append(self.getTableName("index1"+colName))
        self.op.indexTable(tabsCreated[-2], tabsCreated[-1], statType)

        #group by
        tabsCreated.append(self.getTableName("groupby"+colName))
        evalStrs = ["count({})".format(statType)]
        newFields = ["{}_stats".format(colName)]
        self.op.groupBy(tabsCreated[-2], tabsCreated[-1], evalStrs, newFields)
        self.cleanUp(tabsCreated[:-1])
        return tabsCreated[-1]

    def joinAllTabs(self, tabs, allCols):
        if len(tabs) == 0:
            return
        elif len(tabs) == 1:
            return tabs[0]
        tempTab = None
        statType = "StatType_"
        finalStatsCol = None
        for idx in range(1, len(tabs)):
            lTab = tempTab
            lCol = statType+str(idx-1)
            if not lTab:
                lTab = tabs[0]
                lCol = statType + allCols[0]
            rTab = tabs[idx]
            rCol = statType + allCols[idx]
            joinTab = "{}_joinTab_{}".format(lTab, idx)
            self.op.join(lTab, rTab, joinTab,
                        joinType = JoinOperatorT.FullOuterJoin)

            #map
            evalStr = "ifStr(exists({0}), {0}, {1})".format(lCol, rCol)
            mapTab = "mapTab_{}_join_{}".format(allCols[idx], idx)
            finalStatsCol = statType+str(idx)
            if idx == len(tabs) - 1:
                finalStatsCol = "ColumnName"
            self.op.map(joinTab, mapTab, evalStr, finalStatsCol)
            self.op.dropTable(joinTab)

            #index
            indexTab = "tempTab_{}".format(idx)
            self.op.indexTable(mapTab, indexTab, finalStatsCol)
            self.op.dropTable(mapTab)
            if tempTab:
                self.op.dropTable(lTab)
            tempTab = indexTab

        ##project to remove extra cols
        finalTab = "{}_population_stats".format(self.tableName)
        colsToKeep = [col+"_stats" for col in allCols]
        colsToKeep.insert(0, finalStatsCol)
        self.op.project(tempTab, finalTab, colsToKeep)
        self.op.dropTable(tempTab)

        return finalTab

    def cleanUp(self, tables, datasets=[]):
        for tab in tables:
            self.op.dropTable(tab)
        for dataset in datasets:
            dataset.delete()

    def getTableName(self, opName):
        return "{}_{}_{}".format(self.tableName, opName, int(time.time()*100))

    def genPopulationStats(self, tab, metaTab=None):
        print("Generating population stats for", self.tableName)
        fieldStatsTabs = []
        colsToRetain = []
        allCols = []
        for col in self.schema["fields"]:
            fieldStatsTabs.append(self.genColPopStats(tab, col['fieldname']))
            colsToRetain.append("{}_stats".format(col['fieldname']))
            allCols.append(col['fieldname'])

        finalTab = self.joinAllTabs(fieldStatsTabs, allCols)
        fieldStatsTabs.append(finalTab)
        colsToRetain.insert(0, "ColumnName")
        #union
        #fieldStatsTabs.append(self.getTableName("union_pop_stats"))
        #def union(self, sources, dest, columns, dedup=False, unionType=UnionOperatorT.UnionStandard):
        # self.op.union()
        self.makeBDF("_population_stats", [finalTab],
                [colsToRetain])

        self.cleanUp(fieldStatsTabs)
        print("Done!")
        print("*"*60)

    def unionTabs(self, tab, metaTab):
        columns = []
        metaColumns = []
        statsColumns = []
        for col in self.schema["fields"]:
            metaCol = col['fieldname']+'_meta'
            statCol = col['fieldname']+'_stats'
            metaColumns.append(XcalarApiColumnT(metaCol, statCol, 'DfString'))
            statsColumns.append(XcalarApiColumnT(statCol, statCol, 'DfString'))

    def genMaxMinStats(self, tab):
        print("Generating min/max stats for", self.tableName)
        tabsCreated = []
        colsToRetain = []
        evalStrs = []
        newFields = []
        for col in self.schema["fields"]:
            colName = col['fieldname']
            if col['xType'] == 'DfInt64' or col['xType'] == 'DfFloat64':
                colsToRetain.append(colName)
                continue
            elif col['type'] == 'date' or col['type'] == 'timestamp':
                evalStrs.append("float(default:convertToUnixTS({}, None))".\
                    format(colName))
                newFields.append(colName+"_unixts")
            else:
                evalStrs.append("len({})".format(colName))
                newFields.append(colName+"_len")
            # self.op.map(tab, tabsCreated[-1], evalStr, newField)
        if len(evalStrs) > 0:
            #map
            tabsCreated.append(self.getTableName("map_minmax"))
            self.op.map(tab, tabsCreated[-1], evalStrs, newFields)
            #project
            tabsCreated.append(self.getTableName("project_minmax"))
            colsToRetain.extend(newFields)
            self.op.project(tabsCreated[-2], tabsCreated[-1], colsToRetain)
            tab = tabsCreated[-1]
        del evalStrs
        del newFields

        #= map(convertFromUnixTS(testts, "%Y-%d-%m %H:%M:%S"))
        #Group by
        tabsCreated.append(self.getTableName("groupby_minmax"))
        gEvalStrs = []
        gNewFields = []
        concatEvalStr = []
        explodeEvalStr = []
        concatNewFields = []
        for col in colsToRetain:
            for op in ['min', 'max']:
                gEvalStrs.append("{}({})".format(op, col))
                gNewFields.append("{}_{}".format(col, op))
            concatEvalStr.append("concat(string({}), \"|\",string({}))".format(
                            gNewFields[-2], gNewFields[-1]))
            explodeEvalStr.append("explodeString({}, \"|\")".format(col))
            concatNewFields.append(col)
        self.op.groupBy(tab, tabsCreated[-1],
            gEvalStrs, gNewFields, groupAll = True)
        del gEvalStrs
        del gNewFields

        #concat pair of min max cols with |
        #map
        tabsCreated.append(self.getTableName("concat_minmax"))
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                concatEvalStr, concatNewFields)
        del concatEvalStr

        #project
        tabsCreated.append(self.getTableName("project1_minmax"))
        self.op.project(tabsCreated[-2], tabsCreated[-1], concatNewFields)

        # #explode
        # tabsCreated.append(self.getTableName("explode_minmax"))
        # self.op.map(tabsCreated[-2], tabsCreated[-1],
        #         explodeEvalStr, concatNewFields)

        # #row num
        # tabsCreated.append(self.getTableName("rownum"))
        # self.op.getRowNum(tabsCreated[-2], tabsCreated[-1], "XcRownumXc")

        #map
        tabsCreated.append(self.getTableName("minmax"))
        # evalStr = "ifStr(eq(XcRownumXc, 1), \"Min\", \"Max\")"
        evalStr = "string(\"Min|Max\")"
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                evalStr, "ColumnName")
        colsToRetain.insert(0, "ColumnName")

        #retina
        self.makeBDF("_minmax_stats", [tabsCreated[-1]],
                    [colsToRetain])

        self.cleanUp(tabsCreated[:-1])
        print("Done!")
        print("*"*60)

    def genNumericRangeStats(self, tab):
        print("Generating numeric range stats for", self.tableName)
        tabsCreated = []
        colRangeTabs = []
        for col in self.schema["fields"]:
            colName = col['fieldname']
            if col['xType'] == 'DfInt64' or col['xType'] == 'DfFloat64':
                tabsCreated.append(self.getTableName("map"+colName))
                rangeCol = "NumericRange"
                evalStr = "stats:getRange({})".format(colName)
                self.op.map(tab, tabsCreated[-1], evalStr, rangeCol)
                colRangeTabs.append(self.genColRangeStats(tabsCreated[-1],
                                                colName, rangeCol))
        cols = ['ColumnName', 'NumericRange', 'Min', 'Max', 'Count']
        unionCols = []
        for _ in colRangeTabs:
            colList = []
            for col in cols:
                if col in ['ColumnName', 'NumericRange']:
                    colList.append(XcalarApiColumnT(col,
                                    col, 'DfString'))
                else:
                    colList.append(XcalarApiColumnT(col,
                                    col, 'DfInt64'))
            unionCols.append(colList)
        tabsCreated.append(self.getTableName("numeric_range"))
        self.op.union(colRangeTabs, tabsCreated[-1], unionCols)
        #retina
        self.makeBDF("_numericrange_stats", [tabsCreated[-1]],
                    [cols])

        tabsCreated.extend(colRangeTabs)
        self.cleanUp(tabsCreated)
        print("Done!")
        print("*"*60)

    def genDateRangeStats(self, tab):
        print("Generating date range stats for", self.tableName)
        tabsCreated = []
        colRangeTabs = []
        for col in self.schema["fields"]:
            colName = col['fieldname']
            if col['type'] == 'date' or col['type'] == 'timestamp':
                tabsCreated.append(self.getTableName("map"+colName))
                newCols = ['Year', 'Month']
                evalStrs = ["stats:getYear({})".format(colName),
                        "stats:getMonth({})".format(colName)]
                self.op.map(tab, tabsCreated[-1], evalStrs, newCols)
                colRangeTabs.append(self.genColRangeStats(tabsCreated[-1],
                                                    colName, newCols))
        cols = ['ColumnName', 'Year', 'Month', 'Min', 'Max', 'Count']
        unionCols = []
        for _ in colRangeTabs:
            colList = []
            for col in cols:
                if col in ['ColumnName']:
                    colList.append(XcalarApiColumnT(col,
                                    col, 'DfString'))
                else:
                    colList.append(XcalarApiColumnT(col,
                                    col, 'DfInt64'))
            unionCols.append(colList)
        tabsCreated.append(self.getTableName("dateRange"))
        self.op.union(colRangeTabs, tabsCreated[-1], unionCols)
        #retina
        self.makeBDF("_daterange_stats", [tabsCreated[-1]],
                    [cols])

        tabsCreated.extend(colRangeTabs)
        self.cleanUp(tabsCreated)
        print("Done!")
        print("*"*60)

    def genColRangeStats(self, tab, colName, groupbyCol):
        #Map operation to get range for col
        tabsCreated = []

        ##Do Index for groupby
        tabsCreated.append(self.getTableName("index"+colName))
        self.op.indexTable(tab, tabsCreated[-1], groupbyCol)

        #group by to get min, max, count within the range
        evalStrs = []
        newFields = []
        tabsCreated.append(self.getTableName("groupby"+colName))
        for oper in ['Min', 'Max', 'Count']:
            evalStrs.append("{}({})".format(oper.lower(), colName))
            newFields.append(oper)
        self.op.groupBy(tabsCreated[-2], tabsCreated[-1], evalStrs, newFields)

        #Map to add column name and type and seq
        tabsCreated.append(self.getTableName("addCol"+colName))
        evalStr = "string(\"{}\")".format(colName)
        self.op.map(tabsCreated[-2], tabsCreated[-1], evalStr, 'ColumnName')

        self.cleanUp(tabsCreated[:-1])
        return tabsCreated[-1]

    def makeBDF(self, statName, srcTables, srcColsList):
        dirPath = os.path.dirname(self.datasetPath)
        retName = os.path.basename(dirPath) + statName
        try:
            print("Deleting batch dataflow {} if exists,".format(retName))
            self.retina.delete(retName)
        except:
            pass
        print("Creating batch dataflow {}..".format(retName))
        self.retina.make(retName, srcTables, srcColsList)
        self.addParamsDF(retName, dirPath)

        ##run the bdf
        print("Running batch dataflow {}..".format(retName))
        self.retina.execute(retName, [])

    def addParamsDF(self, retinaName, dirPath):
        retObj = self.retina.getDict(retinaName)
        for node in retObj["query"]:
            if node['operation'] == "XcalarApiBulkLoad":
                node['args']['loadArgs']['sourceArgsList'][0]['path'] = dirPath
                node['args']['loadArgs']['sourceArgsList'][0]['recursive'] = True
            elif node['operation'] == "XcalarApiExport":
                node['args']['targetName'] = self.exportTargetName
                node['args']['fileName'] = retinaName+".csv"
                node['args']['createRule'] = 'deleteAndReplace'
                node['args']['targetType'] = "udf"
        self.retina.update(retinaName, retObj)

    def genMetadata(self, tab):
        tabsCreated = []
        evalStrs = []
        newFields = []
        typeDict = {
            "fieldname":"StatType",
            "type":"TYPE"
        }
        explodeStrs = []
        explodeFields = []
        for idx, col in enumerate([typeDict]+self.schema["fields"]):
            #May need to add more here
            if idx == 0:
                idx = "SEQ"
            strs = []
            strs.append("string(\"{}\")".format(idx))
            strs.append("string(\"{}\")".format(col['type']))
            evalStrs.append("concat("+", \"|\",".join(strs)+")")
            newFields.append("{}_meta1".format(col["fieldname"]))
            explodeStrs.append("explodeString({}, \"|\")".format(newFields[-1]))
            explodeFields.append("{}_meta".format(col["fieldname"]))
        #row gen
        tabsCreated.append(self.getTableName("rowgen_meta"))
        self.op.getRowNum(tab, tabsCreated[-1], "XcRownumXc")
        #filter
        filterStr = "eq(XcRownumXc, 1)".format()
        tabsCreated.append(self.getTableName("filter_meta"))
        self.op.filter(tabsCreated[-2], tabsCreated[-1], filterStr)
        #map
        tabsCreated.append(self.getTableName("map_meta"))
        self.op.map(tabsCreated[-2], tabsCreated[-1], evalStrs, newFields)
        #explode
        tabsCreated.append(self.getTableName("explode_meta"))
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                explodeStrs, explodeFields)
        #project
        tabsCreated.append(self.getTableName("project_meta"))
        self.op.project(tabsCreated[-2], tabsCreated[-1], explodeFields)

        self.cleanUp(tabsCreated[:-1])
        return tabsCreated[-1]

    def run(self):
        try:
            self.op.dropTable('*')
        except:
            pass
        tab = self.loadDataset()
        #metaTab = self.genMetadata(tab)
        self.genPopulationStats(tab)
        self.genMaxMinStats(tab)
        self.genNumericRangeStats(tab)
        self.genDateRangeStats(tab)

def parseArgs(args):
    xcApi = XcalarApi()
    username = args.user
    try:
        session = Session(xcApi, username, username,
                    None, True, sessionName="DataStatsWB")
    except Exception as e:
        print("Could not set session for %s" % (username))
        raise e
    xcApi.setSession(session)
    return xcApi

if __name__ == '__main__':
    argParser = argparse.ArgumentParser(description="Prime a Xcalar Workbook with the datasets required for the credit score demo")
    argParser.add_argument('--user', '-u', help="Xcalar User", required=True, default="admin")
    argParser.add_argument('--importTargetName', '-i', help="import target name", required=True)
    argParser.add_argument('--datasetPath', '-d', help="dataset path", required=True)
    argParser.add_argument('--scehmaPath', '-s', help="dataset path", required=True)
    argParser.add_argument('--exportPath', '-e', help="dataset path", required=True)
    args = argParser.parse_args()

    xcApi = parseArgs(args)
    statsGen = DataStatsGenerator(xcApi, args)
    statsGen.run()
