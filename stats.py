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
def __getNumZeroInFracPart(x):
    if x == 0 or x >= 1 or x <= -1:
        return 0
    numZeros = 0
    frac_part = abs(x) - int(abs(x))
    while int(frac_part * 10) == 0:
        frac_part *= 10
        numZeros += 1
    return numZeros

def __getRangeInt(num):
    if num > 0:
        d = int(math.log10(num))
        return '1{0} - 1{1}'.format('0'*d, '0'*(d+1))
    d = len(str(-num)) - 1
    return '-1{0} - -1{1}'.format('0'*(d+1), '0'*d)

def __getRangeltZero(num):
    if num >= 0:
        d = int(math.log10(num))
        if d == 0:
            return '0.{0}1 - 1'.format('0'*d)
        return '0.{0}1 - 0.{1}1'.format('0'*d, '0'*(d-1))
    d = len(str(-num)) - 1
    if d == 0:
        return '-1 - -0.{0}1'.format('0'*d)
    return '-0.{0}1 - -0.{1}1'.format('0'*(d-1), '0'*d)
'''
-------------------------
nums >= 1 and nums <= -1
.
.
(-100 - -10]
(-10 - -1]
.
.
[1 - 10)
[10 - 100)
.
.
-------------------------
nums < 1 and nums > -1
.
.
(-1 - -0.1]
(-0.1 - -0.01]
.
.
[0.01 - 0.1)
[0.1 - 1)
.
.
'''
def getRange(num):
    if num == 0:
        return '0'
    if num >= 1 or num <= -1:
        return __getRangeInt(int(num))
    else:
        d = __getNumZeroInFracPart(num) + 1
        n = int('1'*d)
        return __getRangeltZero(-n if num < 0 else n)

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

