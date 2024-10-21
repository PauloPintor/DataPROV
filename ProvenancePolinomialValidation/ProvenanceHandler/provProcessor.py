import re

def isNumber(s):
    try:
        float(s)  # Try converting to float
        return True
    except ValueError:
        return False

def splitByTokenPLUS(polynomial):
	return [t.strip() for t in re.split(r'[+-]',polynomial)]

def splitByTokenMULT(expression):
	while expression[0] == '(' and expression[-1] == ')':
		expression = expression[1:-1]

	tokens = [t.strip() for t in expression.split("*")]
	return [t for t in tokens if isNumber(t) == False and len(t) > 0]

def composeSQLValidationStatement(token, variableDict, columns, columnsexp):
	tablesList = []
	i = 1
	variableDict2 = {}
	try:
		i += 1
		for t in token:
			table = variableDict[t][0]
			if table not in tablesList:
				tablesList.append(table)
				variableDict2[t] = (table, variableDict[t][1])
			else:
				i += 1
				table = table +' ' +table[0] + str(i)
				variableDict2[t] = (table[0] + str(i), variableDict[t][1])
				tablesList.append(table)
	except Exception as e:
		print("Error: ", e)

	tables = ', '.join('%s' % t for t in tablesList)
	conditions = " AND ".join("%s" % variableDict2[t][0] + ".prov = '" + variableDict2[t][1] + "'" for t in token)
	if columnsexp != []:
		regularColumns = ', '.join('%s' % t for t in columnsexp)
	else:
		regularColumns = ', '.join('%s' % t for t in columns[0])
	aggregationColumns = ', '.join('%s' % t for t in columns[1])

	statm = f"""
		SELECT  {regularColumns.rstrip(', ')}
		FROM    {tables}  
		WHERE   1 = 1 AND {conditions}
	"""
	return statm

