import re
import sympy

import PolynomialHelper.mapTokens as mt


def solveSymbolicExpression(expression):

    pattern = r"[=<>!]+"
    mathSymbol = re.findall(pattern, expression)
    exp = mt.replace_words_with_fixed_number(expression)
    isNotExp = "= 1 " + mathSymbol[0] in exp

    # Regex pattern: Match '.' only when it's surrounded by non-digits (words, spaces, etc.)
    pattern = r"(?<=\D)\.(?=\D)"  # Ensures dot is not part of a float

    # Replace matched dots with '*'
    exp = re.sub(pattern, "*", exp)

    # Split the equation into LHS and RHS
    lhs_str, rhs_str = exp.split(mathSymbol[0])
    rhs = 0
    if "+min" in lhs_str:
        lhs = min(extract_numbers(lhs_str))

        if not isNotExp:
            rhs_str = rhs_str.replace("+min", "+")
            rhs_str = rhs_str.replace("⊗", " * ")

        rhs = eval(rhs_str.strip())
    elif "+max" in lhs_str:
        lhs = max(extract_numbers(lhs_str))

        if not isNotExp:
            rhs_str = rhs_str.replace("+max", "+")
            rhs_str = rhs_str.replace("⊗", " * ")

        rhs = eval(rhs_str.strip())
    else:
        # Evaluate both sides
        lhs_str = lhs_str.replace("⊗", " * ").replace("⊕", "+")

        lhs_str = (
            lhs_str.replace("+sum", "+").replace("+avg", "+").replace("+count", "+")
        )
        lhs = eval(lhs_str.strip())  # Strip whitespace and evaluate
        if not isNotExp:
            rhs_str = rhs_str.replace("⊗", " * ").replace("⊕", "+")
            rhs_str = (
                rhs_str.replace("+sum", "+").replace("+avg", "+").replace("+count", "+")
            )
        else:
            rhs = eval(rhs_str.strip())
        rhs = eval(rhs_str.strip())
    # Check equality
    if mathSymbol[0] == "=":
        return lhs == rhs
    elif mathSymbol[0] == ">":
        return lhs > rhs
    elif mathSymbol[0] == "<":
        return lhs < rhs
    elif mathSymbol[0] == ">=":
        return lhs >= rhs
    elif mathSymbol[0] == "<=":
        return lhs <= rhs
    elif mathSymbol[0] == "!=":
        return lhs != rhs


def extract_numbers(expression):
    """
    Extracts all numeric values from the given string and converts them to floats.

    Args:
        expression (str): The input string.

    Returns:
        list: A list of extracted numbers as floats.
    """
    # Regex pattern to match numbers (including decimals)
    pattern = r"⊗\s*(\d+\.?\d*)"

    # Find all matches
    numbers = re.findall(pattern, expression)

    # Convert to float
    return [float(num) for num in numbers]


def replace_parentheses_with_one(expression):
    while "(" in expression:  # Keep replacing until no parentheses remain
        expression = re.sub(r"\([^()]*\)", "1", expression)
    return expression


def expandPolynomial(prov):
    # TODO: The polynomials can be in a form of \otimes or \oplus
    prov = prov.replace("δ", "")
    result, map = mt.replace_words_with_tokens(prov)
    result = result.replace(".", "*")

    if "* (" not in result:
        return result, map
    else:
        expr = sympy.expand(result)

        # Regex pattern to find variables with exponents (e.g., x2**2)
        pattern = r"(\w+)\*\*(\d+)"

        # Replacement function to expand exponents
        def replace(match):
            base = match.group(1)  # Variable name (e.g., x2)
            exponent = int(match.group(2))  # Exponent value (e.g., 2)
            return "*".join([base] * exponent)  # Expand x2**2 → x2*x2

        # Replace using regex substitution
        expanded_expr = re.sub(pattern, replace, str(expr))

        return expanded_expr, map
