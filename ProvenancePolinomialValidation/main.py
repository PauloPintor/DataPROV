import json
import re
import pandas as pd
import sympy
import sys
import numpy as np

import DatabaseHelper.dbConnectionHelper as db
import PolynomialHelper.solvePolynomials as poly
import PolynomialHelper.solveProvenance as prov
import PolynomialHelper.mapTokens as mt


def validateRow(prov):
    # Example string (shortened for readability)

    # Split while keeping the closing bracket "]"
    parts = re.split(r"]\s+\+", prov)

    # Add back the "]" at the end of each split part except the last one (it already has it)
    parts = [part + "]" if not part.endswith("]") else part for part in parts]
    parts[0] = parts[0][2:]
    parts[-1] = parts[-1][:-2]
    if len(parts) > 1:
        newProv = []
        newAgg = []
        for part in parts:
            pattern = r"\. \[(.*?)\]"
            symbExpressions = re.findall(pattern, part)

            if len(symbExpressions) > 0:
                for expression in symbExpressions:
                    if poly.solveSymbolicExpression(expression):
                        newProv.append(removeSymbols(part))
                    else:
                        newAgg.append(removeSymbols(part))
            else:
                return True, [], []

        return True, newProv, newAgg
    else:
        # Regex pattern
        pattern = r"\. \[(.*?)\]"

        # Find all matches
        symbExpressions = re.findall(pattern, prov)

        if len(symbExpressions) > 0:
            for expression in symbExpressions:
                return poly.solveSymbolicExpression(expression), [], []
        else:
            return True, [], []

    return False, [], []


def solveAggRowsNested(colAgg, newProvAgg):
    colAgg = colAgg.replace("δ", "")
    colAgg = removeSymbols(colAgg)
    for expression in newProvAgg:
        expression = expression.replace("δ", "").strip()
        colAgg = colAgg.replace(expression, "0")

    #    with open("output.txt", "w") as f:
    #        f.write(f"{colAgg}\n")
    # TODO: improve it
    return solveAggRows(colAgg)


def solveAggRows(colAgg):
    if "+min" in colAgg or "+max" in colAgg:
        return min(poly.extract_numbers(colAgg))
    else:
        colAgg = (
            colAgg.replace("⊗", "*")
            .replace("+sum", "+")
            .replace("+avg", "+")
            .replace("+count", "+")
            .replace(" . ", " * ")
        )
        exp = mt.replace_words_with_fixed_number(colAgg)

        exp = exp.replace("(0 )", "0")
        exp = poly.replace_parentheses_with_one(exp)

        result = 0
        for expression in exp.split("+"):
            result += eval(expression)

        return result


def evaluate_nested_expression(expression):
    while "(" in expression:  # While there are still parentheses
        expression = re.sub(
            r"\([^()]*\)",  # Find innermost parentheses
            lambda x: str(
                eval(x.group(0))
            ),  # Evaluate them #lambda x: str(eval(x.group(0))),  # Evaluate them
            expression,
        )
    return expression


def split_expression(expression, chunk_size=10):
    tokens = re.findall(r"\d+|\+|\-|\*|\/|\(|\)", expression)  # Tokenize expression
    chunks = [
        " ".join(tokens[i : i + chunk_size]) for i in range(0, len(tokens), chunk_size)
    ]
    return chunks


def removeSymbolsKeepNumber(prov):
    # Regular expression pattern
    # pattern = r"⊗\s*([\d]+(?:\.\d+)?)"

    # Find all matches
    # matches = re.findall(pattern, prov)

    # Print results
    # number = matches[-1]

    # newProv = removeSymbols(prov)
    # return newProv + " ⊗ " + number
    # Regular expression pattern
    pattern = r"([\d]+(?:\.\d+)?(?:/\d+)*)\s*(?=<)"

    # Find all matches
    matches = re.findall(pattern, prov)
    print(matches)


def removeSymbols(prov):
    # Regex pattern: Match '.' only when it's surrounded by non-digits (words, spaces, etc.)
    pattern = r"\. \[(.*?)\]"

    # Use re.sub to replace matches with an empty string
    cleaned_text = re.sub(pattern, "", prov)

    return cleaned_text.strip()  # Remove any leading/trailing spaces


def resultEqual(query, res, resColNames):
    # TODO: If the original has limit, and order by, it is possible to reduce res
    result = db.executeQuery(query)
    if result is not None:
        resOriginal, resOriginalCols = result
        resOriginal = pd.DataFrame(resOriginal, columns=resOriginalCols)
        resProv = pd.DataFrame(res, columns=resColNames)

        # Drop unwanted columns
        filterCols = ["prov"]
        if "cntprov" in resProv.columns:
            filterCols.append("cntprov")
        resProv_filter = resProv.drop(columns=filterCols, errors="ignore")

        # resProv_filter = (
        #    resProv_filter.groupby("c_count")["custdist"].sum().reset_index()
        # )

        # Ensure columns are in the same order
        resProv_filter = resProv_filter[resOriginal.columns]

        # Get numeric and non-numeric columns
        numeric_cols = resOriginal.select_dtypes(include=[np.number]).columns
        non_numeric_cols = resOriginal.select_dtypes(exclude=[np.number]).columns

        # Convert numeric columns to float
        resOriginal[numeric_cols] = resOriginal[numeric_cols].astype(float)
        resProv_filter[numeric_cols] = resProv_filter[numeric_cols].astype(float)

        if not numeric_cols.empty:
            resOriginal = resOriginal.sort_values(by=list(numeric_cols)).reset_index(
                drop=True
            )
            resProv_filter = resProv_filter.sort_values(
                by=list(numeric_cols)
            ).reset_index(drop=True)

            # Compare numeric columns using np.allclose()
            numeric_equal = np.allclose(
                resOriginal[numeric_cols], resProv_filter[numeric_cols], equal_nan=True
            )
        else:
            numeric_equal = True

        print(resOriginal)
        print(resProv_filter)

        if not non_numeric_cols.empty:
            resOriginal = resOriginal.sort_values(
                by=list(non_numeric_cols)
            ).reset_index(drop=True)
            resProv_filter = resProv_filter.sort_values(
                by=list(non_numeric_cols)
            ).reset_index(drop=True)
            # Compare non-numeric columns using .equals()
            non_numeric_equal = resOriginal[non_numeric_cols].equals(
                resProv_filter[non_numeric_cols]
            )
        else:
            non_numeric_equal = True

        return numeric_equal and non_numeric_equal

    return False


# Load JSON from a file
with open("QueriesAgg.json", "r") as file:
    data = json.load(file)  # Parse JSON into a Python dictionary


for query_id, query_data in data.items():
    print(f"Query {query_id}:")
    result = db.executeQuery(query_data["provenancequery"])
    if result is not None:
        res, resColNames = result
        newRes = []
        indexProv = resColNames.index("prov")
        indexes = []
        for row in res:
            isTrue, newProv, newProvAgg = validateRow(row[indexProv])

            if isTrue:
                row = list(row)
                if len(newProv) > 0:
                    row[indexProv] = " + ".join(newProv)
                else:
                    row[indexProv] = removeSymbols(row[indexProv])
                if len(query_data["columnsagg"]) > 0:
                    for col in query_data["columnsagg"]:
                        indexes = [
                            i for i, value in enumerate(resColNames) if value == col
                        ]
                        if len(indexes) > 1:
                            resultDivison = solveAggRows(
                                row[indexes[0]]
                            ) / solveAggRows(row[indexes[1]])
                            row[indexes[0]] = resultDivison
                            row.pop(indexes[1])
                        else:
                            if (
                                len(query_data["columnsagg"]) > 1
                                and len(newProvAgg) > 0
                            ):
                                for index in indexes:
                                    row[index] = solveAggRowsNested(
                                        row[index], newProvAgg
                                    )
                            else:
                                indexCol = indexes[0]
                                row[indexCol] = solveAggRows(row[indexCol])
                row = tuple(row)
                newRes.append(row)

        resColNamesTemp = resColNames.copy()
        if len(indexes) > 1:
            resColNamesTemp.pop(indexes[1])
        if resultEqual(query_data["originalquery"], newRes, resColNamesTemp):
            for row in newRes:
                try:
                    if prov.solve_provenance(
                        query_data["tables"],
                        query_data["columns"],
                        row,
                        resColNamesTemp,
                        query_data["columnsagg"],
                    ):
                        print("Results are equal")
                    else:
                        print("Results are not equal")
                except Exception as e:
                    print(f"An error occurred: {e}")
        else:
            print("Results are not equal")

    """
	print("  Original Query:", query_data["originalquery"])
	print("  Provenance Query:", query_data["provenancequery"])
	print("  Columns:", query_data["columns"])
	print("  Aggregated Columns:", query_data["columnsagg"])
	print()
	"""
