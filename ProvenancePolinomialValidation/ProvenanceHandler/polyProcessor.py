import sympy
import re

def expansion(provrow):
	pattern = r'\b\w+:\w+[ )]'
	i = 0
	variables = {}
	matches = re.findall(pattern, provrow)
	for match in matches:
		if match[-1] == ' ':
			prov = 'x'+str(i)+' '
			prov2 = 'x'+str(i)
			newMatch = match[:-1]
		elif match[-1] == ')':
			prov = 'x'+str(i)+')'
			prov2 = 'x'+str(i)
			newMatch = match[:-1]
		provrow = provrow.replace(match, prov)
		
		variables[prov2] = tuple(newMatch.split(":"))
		i+=1

	return sympy.expand(provrow), variables

def noExp(provrow):
	pattern = r'\b\w+:\w+[ )]'
	i = 0
	variables = {}
	matches = re.findall(pattern, provrow)
	for match in matches:
		if match[-1] == ' ':
			prov = 'x'+str(i)+' '
			prov2 = 'x'+str(i)
			newMatch = match[:-1]
		elif match[-1] == ')':
			prov = 'x'+str(i)+')'
			prov2 = 'x'+str(i)
			newMatch = match[:-1]
		#provrow = re.sub(match, prov, provrow)
		provrow = provrow.replace(match, prov)
		
		variables[prov2] = tuple(newMatch.split(":"))
		i+=1

	return provrow, variables
	
def value(expression, val):

	# Assumes all variables are equal to 1
	pattern = r'\b(\w+:\w+)\b'

	# Find all occurrences of the pattern
	provrow = re.sub(pattern, '1', expression)


	return(sympy.expand(provrow))