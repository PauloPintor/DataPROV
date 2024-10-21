import Database.dbFunctions as db
import Examples.queriesWithProvenance as examples
import ProvenanceHandler.provProcessor as prov
import ProvenanceHandler.polyProcessor as poly
import re
import math


for example in range(1, 37):
	# List the numbers of the queries to validate
	print(f'>>> Query {example}...', end = ' ')
	query, columns = examples.getQuery(example)

	# Query result
	res, colnames = db.executeQuery(query)

	# Process provenance polynomial
	valid = True
	for row in range(len(res)):
		newrow = res[row][-1].replace('\u2295', ' + ').replace('\u2297', ' * ')

		#res_prov = re.sub(r"\.\s*(min|max|count|sum|avg)?\s*[- ]\d+(\.\d+)?([eE][-+]?\d+)?", "", newrow)
		if '/' in newrow:
			res_prov = re.sub(r"\.\s*(min|max|count|sum|avg)?\s*\d+(\.\d+)?([eE][-+]?\d+)?/\d+(\.\d+)?", "", newrow)
		else:
			res_prov = re.sub(r"\.\s*(min|max|count|sum|avg)?\s*\d+(\.\d+)?([eE][-+]?\d+)?", "", newrow)
		
		if '* (' in res_prov:
			expandedPolynomial, variableDict = poly.expansion(res_prov)
			tokensPLUS = prov.splitByTokenPLUS(str(expandedPolynomial))
		else:
			expandedPolynomial, variableDict = poly.noExp(res_prov)
			tokensPLUS = prov.splitByTokenPLUS(expandedPolynomial)
		# tests if the 'regular' column values can be derived from the provenance polynomial.
		index_contador = -1
		if 'contador' in colnames:
			total = poly.value(res_prov, 1)
			index_contador = colnames.index('contador')

		if index_contador > 0:
			if res[row][index_contador] != total:
				valid = False
				print(f'  --> Error in line {row}!')
				print(f'      {res[row][index_contador]}  >>> contador >>>  {total}')
				break
		
		for token in tokensPLUS:
			
			if columns[0] != []:
				sql = prov.composeSQLValidationStatement(prov.splitByTokenMULT(token), variableDict, columns, columnsexp)
				print(sql)
				provResultTokenPLUS, col_names = db.executeQuery(sql)
				print(provResultTokenPLUS)
				
				fails = 0
				for col in range(len(columns[0])):
					index_col = colnames.index(columns[0][col])
					if res[row][index_col] != provResultTokenPLUS[0][col]:
						fails += 1
				#fails = sum(1 for col in range(len(columns[0])) if res[row][col] != provResultTokenPLUS[0][col])

				if fails > 0:
					print(f'  --> Error in line {row}!')
					print(f'      {res[row]}  >>> regular columns >>>  {provResultTokenPLUS}')
					valid = False
					break



		# Tests if aggregation columns can be derived from the provenance polynomial.
		# The code below works only for queries with a single aggregation column.
		if len(columns[1]) > 0:
			pattern = r'\.(min|max|count|sum|avg)?\s*(-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?)'
			matches = re.findall(pattern, res[row][-1])
			
			if 'count' in matches[0]:
				filtered_list = [item for item in matches if '0' not in item]
				indexColumn = colnames.index(columns[1][0])
				countResult = res[row][indexColumn]
				print(countResult, ' - ' ,len(filtered_list))
				if countResult != len(filtered_list):
					valid = False
					print(f'  --> Error in line {row}!')
					print(f'      {res[row][index_contador]}  >>> aggregation >>>  {len(filtered_list)}')
					break
			elif 'min' in matches[0] or 'max' in matches[0]:
				indexColumn = colnames.index(columns[1][0])
				minMaxResult = res[row][indexColumn]
				print(minMaxResult, ' - ' ,matches[0][1])
				if float(minMaxResult) != float(matches[0][1]):
					valid = False
					print(f'  --> Error in line {row}!')
					print(f'      {minMaxResult}  >>> aggregation >>>  {matches[0][1]}')
					break
			elif 'avg' in matches[0]:
				pattern = r'(\d+(\.\d+)?)/(\d+(\.\d+)?)'
				matches2 = re.findall(pattern, res[row][-1])
				total = 0
				for match in matches2:
					total += float(match[0]) / float(match[2])
				index_contador = colnames.index(columns[1][0])
				somaOriginal = res[row][index_contador]
				_isclose = math.isclose(somaOriginal, total, rel_tol=1e-9)
				print(somaOriginal, ' - ' ,total)
				print(_isclose)
				if _isclose != True:
					valid = False
					print(f'  --> Error in line {row}!')
					print(f'      {res[row][index_contador]}  >>> aggregation >>>  {total}')
					break
			else:
				pattern = r'(\d+(\.\d+)?)/(\d+(\.\d+)?)'
				matches2 = re.findall(pattern, newrow)
				
				total = 0
				conta2 = ''
				if len(matches2) > 0:
					for match in matches2:
						total += float(match[0]) / float(match[2])
				else:
					for match in matches:
						total += float(match[1])

				
				index_contador = colnames.index(columns[1][0])
				somaOriginal = res[row][index_contador]
				_isclose = math.isclose(somaOriginal, total, rel_tol=1e-9)
				print(somaOriginal, ' - ' ,total)
				print(_isclose)
				if _isclose != True:
					valid = False
					print(f'  --> Error in line {row}!')
					print(f'      {res[row][index_contador]}  >>> aggregation >>>  {total}')
					break
			
	if valid:
		print('provenance is Valid.')
	break