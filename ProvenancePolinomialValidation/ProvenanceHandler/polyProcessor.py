import sympy
import re

def expansion(expression):
    pattern = r'\b(\w+:\w+)\b'

    # Find all occurrences of the pattern
    matches = re.findall(pattern, expression)

    # Create a dictionary to map each unique match to a variable (x1, x2, x3, ...)
    variables = {}
    for i, match in enumerate(sorted(set(matches)), start=1):  # sorted to keep order, set to avoid duplicates
    #    variable_dict[match] = f'x{i}'
        expression = expression.replace(match, f'x{i}')
        variables[f'x{i}'] = tuple(match.split(":"))

    return sympy.expand(expression), variables
    

def value(expression, val):
    # Assumes all variables are equal to 1
    pattern = r'\b(\w+:\w+)\b'

    # Find all occurrences of the pattern
    matches = re.findall(pattern, expression)

    for i, match in enumerate(sorted(set(matches)), start=1):  # sorted to keep order, set to avoid duplicates
        expression = expression.replace(match, str(val))

    return(sympy.expand(expression))
