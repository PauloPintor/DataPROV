import Database.dbFunctions as db
import Examples.queriesWithProvenance as examples
import ProvenanceHandler.provProcessor as prov
import ProvenanceHandler.polyProcessor as poly

for example in [1, 2]:             # List the numbers of the queries to validate
    print(f'>>> Query {example}...', end = ' ')
    query, columns, polynomial = examples.getQuery(example)

    # Query result
    res = db.executeQuery(query)

    # Process provenance polynomial
    valid = True
    for row in range(len(res)):
        expandedPolynomial, variableDict = poly.expansion(polynomial[row])
        # print(res[row], "->", polynomial[row])
        tokensPLUS = prov.splitByTokenPLUS(str(expandedPolynomial))

        # tests if the 'regular' column values can be derived from the provenance polynomial.
        for token in tokensPLUS:
            sql = prov.composeSQLValidationStatement(prov.splitByTokenMULT(token), variableDict, columns)
            provResultTokenPLUS = db.executeQuery(sql)

            fails = sum(1 for col in range(len(columns[0])) if res[row][col] != provResultTokenPLUS[0][col])  
            if fails > 0:
                print(f'  --> Error in line {row}!')
                print(f'      {res[row]}  >>> regular columns >>>  {provResultTokenPLUS}')
                valid = False
                break

        # Tests if aggregation columns can be derived from the provenance polynomial.
        # The code below works only for queries with a single aggregation column.
        if res[row][-1] != poly.value(polynomial[row], 1):
            valid = False
            print(f'  --> Error in line {row}!')
            print(f'      {res[row]}  >>> aggregation >>>  {poly.value(polynomial[row], 1)}')
    if valid:
        print('provenance is Valid.')








