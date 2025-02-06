import sys
import argparse
import os
import subprocess
import re

try:
    # Try importing SymEngine
    import sympy as sp
except ImportError:
    # If SymEngine is not installed, install it
    subprocess.check_call(['pip3', 'install', 'sympy'])
    
def parse_input(input_str):
	print("input_str"+input_str)
	# Extract variable names from the input string
	sets = re.findall(r'\{([^}]*)\}', input_str)
	return [set(map(str.strip, s.split(','))) for s in sets]

def create_boolean_expression(variable_sets):
	# Create a dictionary to hold the sympy symbols
	symbols_dict = {}

	# Create sympy symbols for each unique variable
	for var_set in variable_sets:
		for var in var_set:
			if var not in symbols_dict:
				symbols_dict[var] = sp.symbols(var)

	# Create boolean expressions for each set
	expressions = []
	for var_set in variable_sets:
		if len(var_set) == 1:
			# Single variable, just use it directly
			expressions.append(symbols_dict[list(var_set)[0]])
		else:
			# Multiple variables, use AND
			expr = sp.And(*[symbols_dict[var] for var in var_set])
			expressions.append(expr)

	# Combine all expressions using OR
	final_expr = sp.Or(*expressions)
	return final_expr

def main():
	# Check if the argument is passed

	if len(sys.argv) > 1:
		# Create the parser
		parser = argparse.ArgumentParser(description="Process some arguments.")

		# Add arguments
		parser.add_argument('-path', type=str, required=True, help='Path argument')
		parser.add_argument('-wp', type=str, required=True, help='Why parameter')
		parser.add_argument('-wb', type=str, required=True, help='Boolean result')
		parser.add_argument('-wt', type=str, required=True, help='Trio result')
		parser.add_argument('-wpos', type=str, required=True, help='Pos result')
	
		# Parse the arguments
		args = parser.parse_args()

		file_path = args.path
		if not os.path.isfile(file_path):
			raise FileNotFoundError(f"File not found: {file_path}")
		else:
			fileProv = open(file_path, 'r')
			expression = fileProv.readlines()[0]
			fileProv.close()
			expanded = False
			if '* (' in expression:
				expanded_expression = sp.expand(expression)
				expanded = True
			else:
				expression = re.sub(r'\s{2,}', ' ', expression)
				expression = expression.replace('(', '').replace(')', '')
				expressions = expression.split(' + ')
				dictExp = {}
				for exp in expressions:
					if exp in dictExp:
						dictExp[exp] += 1
					else:
						dictExp[exp] = 1
				result = []
				resultWhy = []
				for key, count in dictExp.items():
					if count > 1 and (args.wp == "true" or args.wt == "true"):
						result.append(f"{count}*{key}")
					else:
						if args.wb == "true" or args.wt == "true": result.append(key)
						if args.wp == "true" or args.wpos == "true": resultWhy.append(key)
				expanded_expression = ' + '.join(result)
			pattern = r"\*\*\d+"
			boolean_provenance = ''
			trio_provenance = ''
			why_provenance = ''
			postbool_provenance = ''
			equalProvenance = False

			if args.wb == "true":
				if expanded:
					boolean_provenance = ' + '.join(str(term) for term in expanded_expression.as_coefficients_dict())
				else:
					boolean_provenance = expanded_expression
			if args.wt == "true":
				trio_provenance = re.sub(pattern, '', str(expanded_expression))
			if args.wp == "true":
				
				if args.wb == "true":
					why_provenance = re.sub(pattern, '', str(boolean_provenance))
					why_provenance = why_provenance.replace(' + ', '}, {')
				elif expanded:
					why_provenance = ' + '.join(str(term) for term in expanded_expression.as_coefficients_dict())
					why_provenance = re.sub(pattern, '', str(why_provenance))
					why_provenance = why_provenance.replace(' + ', '}, {')
				else:
					why_provenance = '}, {'.join(resultWhy)
				
				why_provenance = why_provenance.replace('*', ',')
				why_provenance = re.sub(r"\{\d+,\s*", "{", why_provenance)
				why_provenance = '{{'+why_provenance+'}}';
			if args.wpos == "true":
				if args.wp == "true":
					postbool_provenance = create_boolean_expression(parse_input(why_provenance))
				elif args.wb == "true":
					if equalProvenance == False:
						postbool_provenance = re.sub(pattern, '', str(boolean_provenance))
					postbool_provenance = postbool_provenance.replace(' + ', '}, {')
					postbool_provenance = postbool_provenance.replace('*', ',')
					postbool_provenance = re.sub(r"\{\d+,\s*", "{", postbool_provenance)
					postbool_provenance = create_boolean_expression(parse_input('{'+postbool_provenance+'}'))
				elif expanded:
					postbool_provenance = ' + '.join(str(term) for term in expanded_expression.as_coefficients_dict())
					postbool_provenance = re.sub(pattern, '', str(postbool_provenance))
					postbool_provenance = postbool_provenance.replace(' + ', '}, {')
					postbool_provenance = re.sub(r"\{\d+,\s*", "{", postbool_provenance)
					postbool_provenance = create_boolean_expression(parse_input('{'+postbool_provenance+'}'))
				else:
					postbool_provenance = '}, {'.join(resultWhy)
					postbool_provenance = postbool_provenance.replace('*', ',')
					postbool_provenance = re.sub(r"\{\d+,\s*", "{", postbool_provenance)
					postbool_provenance = create_boolean_expression(parse_input('{'+postbool_provenance+'}'))
				
				
				postbool_provenance = sp.simplify(postbool_provenance)
				postbool_provenance = str(postbool_provenance)
				postbool_provenance = postbool_provenance.replace('&', '\u2227')
				postbool_provenance = postbool_provenance.replace('|', '\u2228')
				
		
		with open(file_path, 'w') as file:
			file.write(boolean_provenance + '\n')
			file.write(trio_provenance + '\n')
			file.write(why_provenance + '\n')
			file.write(postbool_provenance + '\n')

	else:
		raise ValueError("No variable received from Java")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Exception: {e}")
        sys.exit(1) 