#!/opt/xcalar/opt/xcalar/bin/python3.6

import argparse
import json
import hashlib
import time
import timeit
import traceback
from datetime import datetime

from xcalar.compute.api.XcalarApi import XcalarApi
from xcalar.compute.api.Session import Session
from xcalar.compute.api.WorkItem import *
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
##This needs to changed if we add more tags in population stats udf
numPopStatsTags = 6

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
        self.scehmaPath = args.schemaPath
        self.exportPath = args.exportPath
        self.failSilent = args.failSilent
        self.delDf = args.delDf

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

    def getSampleFileToModelDF(self):
        lstFilesWI = WorkItemListFiles(targetName = self.importTargetName,
                        path=self.datasetPath, pattern='', recursive=True)
        lstFiles = self.xcApi.execute(lstFilesWI)
        ##Return a file which is not a directory
        for file in lstFiles:
            if file.attr.isDirectory or 'mseed' in file.name.lower() :
                continue
            return os.path.join(self.datasetPath, file.name)
        raise ValueError("No files present in {}".format(self.datasetPath))

    def loadDataset(self):
        sampleFile = self.getSampleFileToModelDF()
        print("Loading dataset from {}..".format(sampleFile))
        tabsCreated = []
        timestamp = int(time.time())
        self.tableName = os.path.basename(self.datasetPath)
        self.getSchema()
        datasetName = "{}.{}.{}".format(self.username,
                    timestamp, self.tableName)
        args = {"linesToSkip":0, "schemaMode":"header"}
        if self.schema["filetype"] == "DELIMITED":
            args["fieldDelim"] = self.schema["delimiter"]
        else:
            print("Specifies file type {} not supported".format(
                    self.schema["filetype"]))
        ##XXX: harcoding for now
        args["fieldDelim"] = '\u2566'
        dataset = CsvDataset(self.xcApi, self.importTargetName,
            sampleFile, datasetName, **args)
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
            if col["type"] == 'integer':
                evalStrs.append("int({}::{})".format(datasetName, col['fieldname']))
                col["xType"] = "DfInt64"
            elif col["type"] == 'float' or col["type"] == 'decimal':
                evalStrs.append("float({}::{})".format(datasetName, col['fieldname']))
                col["xType"] = "DfFloat64"
            elif col["type"] == 'date' or col["type"] == 'timestamp':
                evalStrs.append("ifStr(neq({0}::{1}, \"\"), string({0}::{1}), string(XcXc))".format(datasetName, col['fieldname']))
                col["xType"] = "DfString"
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

    def genColPopStats(self, startTab, colStruct):
        tabsCreated = []
        colName = colStruct['fieldname']
        colType = colStruct['type']
        #project
        tabsCreated.append(self.getTableName("project"+colName))
        self.op.project(startTab, tabsCreated[-1], [colName])

        #map
        tabsCreated.append(self.getTableName("map"+colName))
        evalStr = "stats:fieldPopStats({},\"{}\")".format(colName, colType)
        tokensCol = colName+"_stats"
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                evalStr, tokensCol)

        statType = "StatType_"+colName
        # #explode string
        # tabsCreated.append(self.getTableName("explode"+colName))
        # evalStr = "explodeString({}, \"{}\")".format(colName+"_stats", "|")
        # self.op.map(tabsCreated[-2], tabsCreated[-1], evalStr, statType)

        # ##WORKAROUND FOR EXPLODE STRING
        maxNumSplits = numPopStatsTags
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
        self.op.groupBy(tabsCreated[-2], tabsCreated[-1], evalStrs, [colName])
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

        ##Make the table a one row which helps in
        ## transposing the results
        tabsCreated = [tempTab]

        ##project to remove extra cols
        # tabsCreated.append(self.getTableName('index_pop_stats'))
        # allCols.insert(0, finalStatsCol)
        # self.op.project(tempTab, tabsCreated[-1], allCols)

        ##Map to append | to apply listagg
        allCols.insert(0, finalStatsCol)
        tabsCreated.append(self.getTableName('map_pop_stats'))
        evalStrs = ["int(\"1\")"]
        newFields = ["allones"]
        gEvalStrs = []
        for col in allCols:
            evalStr = "ifStr(exists({0}), concat(string({0}), \"|\"), \
            concat(string(\"0\"), \"|\"))".format(col)
            evalStrs.append(evalStr)
            newFields.append(col+"_stats")
            gEvalStrs.append("listAgg({})".format(newFields[-1]))
        self.op.map(tabsCreated[-2], tabsCreated[-1], evalStrs, newFields)

        ##Group by with listagg
        #index on allones which also helps in skewing the records to one bucket
        tabsCreated.append(self.getTableName('index_pop_stats'))
        self.op.indexTable(tabsCreated[-2], tabsCreated[-1], "allones")

        tabsCreated.append(self.getTableName('groupby_pop_stats'))
        self.op.groupBy(tabsCreated[-2], tabsCreated[-1], gEvalStrs, allCols)

        self.cleanUp(tabsCreated[:-1])
        return tabsCreated[-1]

    def cleanUp(self, tables, datasets=[]):
        for tab in tables:
            try:
                self.op.dropTable(tab)
            except:
                pass
        for dataset in datasets:
            try:
                dataset.delete()
            except:
                pass

    def getTableName(self, opName):
        return "{}_{}_{}".format(self.tableName, opName, int(time.time()*100))

    def genPopulationStats(self, tab, metaTab=None):
        print("Generating population stats for", self.tableName)
        fieldStatsTabs = []
        colsToRetain = []
        allCols = []
        for col in self.schema["fields"]:
            fieldStatsTabs.append(self.genColPopStats(tab, col))
            allCols.append(col['fieldname'])

        finalTab = self.joinAllTabs(fieldStatsTabs, allCols)
        fieldStatsTabs.append(finalTab)
        #union
        #fieldStatsTabs.append(self.getTableName("union_pop_stats"))
        #def union(self, sources, dest, columns, dedup=False, unionType=UnionOperatorT.UnionStandard):
        # self.op.union()
        self.cleanUp(fieldStatsTabs[:-1])
        return ("_population_stats", finalTab, allCols)

    def unionTabs(self, tab, metaTab):
        columns = []
        metaColumns = []
        statsColumns = []
        for col in self.schema["fields"]:
            metaCol = col['fieldname']+'_meta'
            statCol = col['fieldname']+'_stats'
            metaColumns.append(XcalarApiColumnT(metaCol, statCol, 'DfString'))
            statsColumns.append(XcalarApiColumnT(statCol, statCol, 'DfString'))

    def genMinMaxStats(self, tab):
        print("Generating min/max stats for", self.tableName)
        tabsCreated = []
        colsToRetain = []
        evalStrs = []
        newFields = []
        for col in self.schema["fields"]:
            colName = col['fieldname']
            if col['xType'] == 'DfInt64' or col['xType'] == 'DfFloat64' or \
                col['type'] == 'date' or col['type'] == 'timestamp':
                colsToRetain.append(colName)
            else:
                evalStrs.append("len({})".format(colName))
                newFields.append(colName+"_len")
            # self.op.map(tab, tabsCreated[-1], evalStr, newField)
        #map
        tabsCreated.append(self.getTableName("map_minmax"))
        ##add one to do groupby on
        evalStrs.append("int(\"1\")")
        newFields.append("XcAllOnesXc")
        self.op.map(tab, tabsCreated[-1], evalStrs, newFields)
        colsToRetain.extend(newFields[:-1])

        #project
        # tabsCreated.append(self.getTableName("project_minmax"))
        # self.op.project(tabsCreated[-2], tabsCreated[-1], colsToRetain)

        ##Do index
        tabsCreated.append(self.getTableName("index_minmax"))
        self.op.indexTable(tabsCreated[-2], tabsCreated[-1], newFields[-1])
        del evalStrs
        del newFields

        #Group by
        tabsCreated.append(self.getTableName("groupby_minmax"))
        gEvalStrs = []
        gNewFields = []
        concatEvalStr = []
        explodeEvalStr = []
        for col in colsToRetain:
            for op in ['min', 'max']:
                gEvalStrs.append("{}({})".format(op, col))
                gNewFields.append("{}_{}".format(col, op))
            concatEvalStr.append("concat(string({}), \"|\",string({}))".format(
                            gNewFields[-2], gNewFields[-1]))
            # explodeEvalStr.append("explodeString({}, \"|\")".format(col))
        self.op.groupBy(tabsCreated[-2], tabsCreated[-1],
            gEvalStrs, gNewFields)
        del gEvalStrs
        del gNewFields

        #concat pair of min max cols with |
        #map
        colsToRetain.insert(0, "ColumnName")
        concatEvalStr.insert(0, "string(\"Min|Max\")")
        tabsCreated.append(self.getTableName("concat_minmax"))
        self.op.map(tabsCreated[-2], tabsCreated[-1],
                concatEvalStr, colsToRetain)
        del concatEvalStr

        #project
        # tabsCreated.append(self.getTableName("project1_minmax"))
        # self.op.project(tabsCreated[-2], tabsCreated[-1], concatNewFields)

        # #explode
        # tabsCreated.append(self.getTableName("explode_minmax"))
        # self.op.map(tabsCreated[-2], tabsCreated[-1],
        #         explodeEvalStr, concatNewFields)

        # #row num
        # tabsCreated.append(self.getTableName("rownum"))
        # self.op.getRowNum(tabsCreated[-2], tabsCreated[-1], "XcRownumXc")

        #map
        # tabsCreated.append(self.getTableName("minmax"))
        # # evalStr = "ifStr(eq(XcRownumXc, 1), \"Min\", \"Max\")"
        # evalStr = "string(\"Min|Max\")"
        # self.op.map(tabsCreated[-2], tabsCreated[-1],
        #         evalStr, "ColumnName")
        # colsToRetain.insert(0, "ColumnName")

        self.cleanUp(tabsCreated[:-1])
        return ('_minmax_stats', tabsCreated[-1], colsToRetain)

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

        if unionCols:
            tabsCreated.extend(colRangeTabs)
            tabsCreated.append(self.getTableName("numeric_range"))
            self.op.union(colRangeTabs, tabsCreated[-1], unionCols)
            self.cleanUp(tabsCreated[:-1])
            return ("_numericrange_stats", tabsCreated[-1], cols)
        else:
            print("No numeric type columns present in schema!")
            print("Not able to create numeric range stats")
            return None

    def genDateRangeStats(self, tab):
        print("Generating date range stats for", self.tableName)
        tabsCreated = []
        colRangeTabs = []
        for col in self.schema["fields"]:
            colName = col['fieldname']
            if col['type'] == 'date' or col['type'] == 'timestamp':
                tabsCreated.append(self.getTableName("map"+colName))
                newCols = ['Year', 'Month', 'Day']
                evalStrs = ["stats:getYear({})".format(colName),
                        "stats:getMonth({})".format(colName),
                        "stats:getDay({})".format(colName)]
                self.op.map(tab, tabsCreated[-1], evalStrs, newCols)
                colRangeTabs.append(self.genColRangeStats(tabsCreated[-1],
                                                    colName, newCols))
        cols = ['ColumnName', 'Year', 'Month', 'Day', 'Min', 'Max', 'Count']
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

        if unionCols:
            tabsCreated.extend(colRangeTabs)
            tabsCreated.append(self.getTableName("dateRange"))
            self.op.union(colRangeTabs, tabsCreated[-1], unionCols)
            self.cleanUp(tabsCreated[:-1])
            return ("_daterange_stats", tabsCreated[-1], cols)
        else:
            print("No date type columns present in schema!")
            print("Not able to create date range stats")
            return None

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

    def makeBDF(self, statNames, srcTables, srcColsList):
        retName = os.path.basename(self.datasetPath) + "_stats"
        try:
            print("Deleting batch dataflow {} if exists,".format(retName))
            self.retina.delete(retName)
        except:
            pass
        print("Creating batch dataflow {}..".format(retName))
        self.retina.make(retName, srcTables, srcColsList)
        self.addParamsDF(retName, statNames)

        ##run the bdf
        print("Running batch dataflow {}..".format(retName))
        self.retina.execute(retName, [])

        if self.delDf:
            print("Deleting batch dataflow {}..".format(retName))
            self.retina.delete(retName)

    def addParamsDF(self, retinaName, statNames):
        retObj = self.retina.getDict(retinaName)
        idx = 0
        for node in retObj["query"]:
            if node['operation'] == "XcalarApiBulkLoad":
                node['args']['loadArgs']['sourceArgsList'][0]['path'] = \
                                                    self.datasetPath
                node['args']['loadArgs']['sourceArgsList'][0]['recursive'] = \
                                                    True
            elif node['operation'] == "XcalarApiExport":
                node['args']['targetName'] = self.exportTargetName
                node['args']['fileName'] = retinaName.split("_")[0] + \
                                            statNames[idx] + ".csv"
                node['args']['createRule'] = 'deleteAndReplace'
                node['args']['targetType'] = "udf"
                node['args']['fieldDelim'] = ","
                node['args']['quoteDelim'] = "\""
                idx += 1
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
        self.cleanUp(['*'])
        tab = self.loadDataset()
        statRes = []
        #metaTab = self.genMetadata(tab)
        try:
            statRes.append(self.genPopulationStats(tab))
        except Exception as e:
            self.handleError(e)
        try:
            statRes.append(self.genMinMaxStats(tab))
        except Exception as e:
            self.handleError(e)
        try:
            res = self.genNumericRangeStats(tab)
            if res:
                statRes.append(res)
        except Exception as e:
            self.handleError(e)
        try:
            res = self.genDateRangeStats(tab)
            if res:
                statRes.append(res)
        except Exception as e:
            self.handleError(e)

        self.makeBDF(*list(zip(*statRes)))
        self.cleanUp(['*'])
        print("Done!")
        print("*"*60)

    def handleError(self, e):
        print("Error", str(e))
        print(traceback.format_exc())
        if not self.failSilent:
            raise e
        print("*"*60)

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
    argParser = argparse.ArgumentParser(description="Data statistics generator")
    argParser.add_argument('--user', '-u', help="Xcalar User", required=True, default="admin")
    argParser.add_argument('--importTargetName', '-i', help="import target name", required=True)
    argParser.add_argument('--datasetPath', '-d', help="dataset path, point to a folder containing files with same schema", required=True)
    argParser.add_argument('--schemaPath', '-s', help="schema path for the dataset ", required=True)
    argParser.add_argument('--exportPath', '-e', help="Export path, to write stats results to", required=False, default="/mnt/xcalar/export")
    argParser.add_argument('--failSilent', help="Silent fail the stats",
        action='store_true')
    argParser.add_argument('--delDf', help="Deletes stats dataflow after execution",
        action='store_true')
    args = argParser.parse_args()

    xcApi = parseArgs(args)
    statsGen = DataStatsGenerator(xcApi, args)
    statsGen.run()
