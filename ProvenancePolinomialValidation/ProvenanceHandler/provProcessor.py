def isNumber(s):
    try:
        float(s)  # Try converting to float
        return True
    except ValueError:
        return False

def splitByTokenPLUS(polynomial):
    return [t.strip() for t in polynomial.split("+")]

def splitByTokenMULT(expression):
    tokens = [t.strip() for t in expression.split("*")]
    return [t for t in tokens if isNumber(t) == False and len(t) > 0]

def composeSQLValidationStatement(token, variableDict, columns):

    tables = ', '.join('%s' % variableDict[t][0] for t in token)
    conditions = " AND ".join("%s" % variableDict[t][0] + ".prov = '" + variableDict[t][1] + "'" for t in token)
    regularColumns = ', '.join('%s' % t for t in columns[0])
    aggregationColumns = ', '.join('%s' % t for t in columns[1])

    statm = f"""
        SELECT  {regularColumns}
        FROM    {tables}  
        WHERE   1 = 1 AND {conditions}
    """
    return statm