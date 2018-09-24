import math
import dateutil.parser

def fieldPopStats(col):
    res = ["TOTAL"]
    try:
        intCol = int(col)
    except:
        intCol = None
    if not col:
        res.append("NIL")
        return "|".join(res)
    if intCol and intCol > 0:
        res.append("POS")
    elif intCol and intCol < 0:
        res.append("NEG")
    elif intCol and intCol == 0:
        res.append("ZERO")
    col = str(col).strip()
    if col == "":
        res.append("SPACE")
    if col:
        res.append("POPULATED")
    return "|".join(res)

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
        return '0.{0}1 - 0.{1}1'.format('0'*d, '0'*(d-1))
    d = len(str(-num)) - 1
    return '-0.{0}9 - -0.{1}1'.format('9'*d, '0'*d)

def getRange(num):
    if num == 0:
        return '0'
    if num >= 1 or num <= -1:
        return __getRangeInt(int(num))
    else:
        d = __getNumZeroInFracPart(num)
        print("Num of zeros", d)
        n = int('1'*d)
        return __getRangeltZero(-n if num < 0 else n)

def getYear(col):
    return dateutil.parser.parse(col).year

def getMonth(col):
    return '{:02}'.format(dateutil.parser.parse(col).month)

