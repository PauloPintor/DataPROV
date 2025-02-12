import itertools
import re
from collections import defaultdict

import DatabaseHelper.dbConnectionHelper as db
import PolynomialHelper.mapTokens as mt
import PolynomialHelper.solvePolynomials as poly


def solve_provenance(tables, columns, row, colsrow, aggcolumns):
    polynomial = row[colsrow.index("prov")]
    expandend_poly, map = poly.expandPolynomial(polynomial)
    joins = [t.strip() for t in expandend_poly.split("+")]

    if len(columns) > 0:
        for join in joins:
            join = join.replace("(", "").replace(")", "")
            pattern = r"^\d+\*\s*"  # Matches a leading number, '*' and optional spaces
            join = re.sub(pattern, "", join)
            tokens = join.split("*")
            tokens = [token.strip() for token in tokens]
            result = conjuntions(tokens, tables, columns, map)
            if result is not None:
                if not compareRows(result[0], columns, row, colsrow, aggcolumns):
                    raise ValueError("Row does not match in conjuntions")

    if "cntprov" in colsrow:
        if not alternatives(expandend_poly, row[colsrow.index("cntprov")]):
            raise ValueError("Error in alternatives" + row)
    else:
        if not alternatives(expandend_poly, 1):
            return False

    return True


def compareRows(row, columnsRow, rowProv, columnsProv, aggcolumns):
    columnsProvTemp = columnsProv.copy()
    rowProv = list(rowProv)

    idxProv = columnsProvTemp.index("prov")

    rowProv.pop(idxProv)
    columnsProvTemp.pop(idxProv)

    idxCount = columnsProvTemp.index("cntprov") if "cntprov" in columnsProv else -1

    if idxCount != -1:
        rowProv.pop(idxCount)
        columnsProvTemp.pop(idxCount)

    for col in aggcolumns:
        idx = columnsProvTemp.index(col)
        rowProv.pop(idx)
        columnsProvTemp.pop(idx)

    rowProv = tuple(rowProv)

    return sorted(map(str, row)) == sorted(map(str, rowProv))
    """
    resOriginal = pd.DataFrame(row, columns=columnsRow)
    resProv = pd.DataFrame(rowProv, columns=columnsProv)

    filterCols = ["prov"]
    if "cntprov" in resProv.columns:
        filterCols.append("cntprov")

    resProv_filter = resProv.drop(columns=filterCols)
    print(resOriginal)
    print(resProv_filter)
    return resOriginal.equals(resProv_filter)
    """


def alternatives(polynomial, count):
    try:
        # Your code that may trigger recursion error
        result = eval(mt.replace_tokens_with_fixed_number(polynomial))
    except RecursionError:
        result = 0
        for exp in polynomial.split("+"):
            result += eval(mt.replace_tokens_with_fixed_number(exp))

    return result == eval(str(count))


"""
def alternatives(polynomial, count):

    # Remove all parentheses and their contents, along with the preceding multiplication symbol
    # Use regex to remove the part including and after the last separator before the first '('
    polynomial = poly.replace_parentheses_with_one(polynomial)

    # polynomial = re.sub(pattern, "", polynomial)
    polynomial = polynomial.replace(".", "*")
    # polynomial = polynomial.replace("δ", "")

    try:
        # Your code that may trigger recursion error
        result = eval(mt.replace_words_with_fixed_number(polynomial))
    except RecursionError:
        result = 0
        for exp in polynomial.split("+"):
            exp = exp.replace("(", "").replace(")", "")
            result += eval(mt.replace_words_with_fixed_number(exp))

    return result == eval(str(count))
"""


def conjuntions(tokens, tables, columns, map):

    reverse_dict = {v: k for k, v in map.items()}
    for t in tokens:
        if t.strip() not in reverse_dict:
            raise ValueError(f"Token {t} not found in map")
        else:
            tokens[tokens.index(t.strip())] = reverse_dict[t.strip()]

    aliased_list, short_alias_list = generate_aliased_arrays(tables)

    # Generate all permutations of Set2
    permutations_of_set2 = itertools.permutations(tokens)

    # Pair each permutation of Set2 with Set1 element-wise
    all_combinations = [
        list(zip(short_alias_list, permutation)) for permutation in permutations_of_set2
    ]

    # Generate conditions for each tuple group
    for group in all_combinations:
        condition_parts = [f"{table}.prov = '{prov}'" for table, prov in group]
        condition_str = " and ".join(condition_parts)
        # conditions.append(f"{condition_str}")
        statm = f"""
        SELECT  {(', ').join(columns)}
        FROM    {(', ').join(aliased_list)}  
        WHERE   {condition_str}
        """
        result = db.executeQuery(statm)
        if result is not None:
            result, _ = result
            if len(result) > 1 or len(result) == 0:
                pass
            else:
                return result


def generate_aliased_arrays(table_list):
    count = defaultdict(int)  # Dictionary to track occurrences
    aliased_list = []
    short_alias_list = []

    for table in table_list:
        count[table] += 1  # Increment occurrence count

        if count[table] == 1:
            # First occurrence, keep original name
            aliased_list.append(table)
            short_alias_list.append(table)
        else:
            # Create alias for duplicate occurrence
            alias = f"{table[0]}{count[table] - 1}"  # Generate alias like "ps1"
            aliased_list.append(f"{table} as {alias}")
            short_alias_list.append(alias)

    return aliased_list, short_alias_list
