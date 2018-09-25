import math
import dateutil.parser

############## Population stats UDF Functions #################
def __tagNumericCol(col):
    if col is None:
        return ["NIL"]
    col = float(col)
    tags = ["POPULATED"]
    if col > 0:
        tags.append("POS")
    elif col < 0:
        tags.append("NEG")
    else:
        tags.append("ZERO")
    return tags

def __tagStringCol(col):
    if col is None:
        return ["NIL"]
    col = str(col).strip()
    if col == "":
        return ["SPACE"]
    return ["POPULATED"]

def fieldPopStats(col, colType='string'):
    tags = ["TOTAL"]
    if colType == 'integer' or colType == 'decimal':
        tags.extend(__tagNumericCol(col))
    elif colType == 'timestamp' or colType == 'date':
        #currently treating timestamp as string
        tags.extend(__tagStringCol(col))
    else:#string case
        tags.extend(__tagStringCol(col))
    return "|".join(tags)

############## Numeric Range stats UDF Functions #################

def __toBucket(n):
   number_digits = math.floor(math.log10(abs(n)))
   # obscure problem with Python decimal precision will cause .3 to be .30000000004
   # the round() below corrects that problem.
   return round(int(n * (10 ** -number_digits)) * (10 ** number_digits), 10)


def __getNumZeroInFracPart(x):
    if x == 0 or x >= 1 or x <= -1:
        return 0
    numZeros = 0
    frac_part = abs(x) - int(abs(x))
    while int(frac_part * 10) == 0:
        frac_part *= 10
        numZeros += 1
    return numZeros

def __getRangeInt(origNum, num):
    bucket = __toBucket(origNum)

    if num > 0:
        d = int(math.log10(num))
        return '1{0} to 1{1} : {2}'.format('0'*d, '0'*(d+1), bucket)
    d = len(str(-num)) - 1
    return '-1{0} to -1{1} : {2}'.format('0'*d, '0'*(d+1), bucket)

def __getRangeltZero(origNum, num):
    bucket = __toBucket(origNum)

    if num >= 0:
        d = int(math.log10(num))
        if d == 0:
            return '0.{0}1 to 1 : {1}'.format('0'*d, bucket)
        return '0.{0}1 to 0.{1}1 : {2}'.format('0'*d, '0'*(d-1), bucket)
    d = len(str(-num)) - 1
    if d == 0:
        return '-1 to -0.{0}1 : {1}'.format('0'*d, bucket)
    return '-0.{0}1 to -0.{1}1 : {2}'.format('0'*(d-1), '0'*d, bucket)
'''
-------------------------
nums >= 1 and nums <= -1
.
.
(-100 to -90], (-90 to -80], ... , (-20 to -10]
(-10 to -9], (-9 to -8], ... , (-2 to -1]
.
.
[1 to 2), [2 to 3), ... , [9 to 10)
[10 to 20), [20 to 30), ... , [90 to 100)
.
.
-------------------------
nums < 1 and nums > -1
.
.
(-1 to -0.9], (-0.9 to -0.8], ... , (-0.1 to -0.01]
.
.
[0.01 to 0.02), [0.02 to 0.03), ... , [0.09 to 0.1)
[0.1 to 0.2), [0.2 to 0.3), ... , [0.9 to 1)
.
.
'''
def getRange(num):
    if num == 0:
        return '0'
    if num >= 1 or num <= -1:
        return __getRangeInt(num, int(num))
    else:
        d = __getNumZeroInFracPart(num) + 1
        n = int('1'*d)
        return __getRangeltZero(num, -n if num < 0 else n)

############## Date Range UDF Functions #################
def getYear(col):
    if not col:
        return 0
    return '{:04}'.format(dateutil.parser.parse(col).year)

def getMonth(col):
    if not col:
        return 0
    return '{:02}'.format(dateutil.parser.parse(col).month)

def getDay(col):
    if not col:
        return 0
    return '{:02}'.format(dateutil.parser.parse(col).day)

