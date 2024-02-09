import subprocess
import pathlib
import os
import re

try:
	# Try importing SymEngine
	import symengine
except ImportError:
	# If SymEngine is not installed, install it
	subprocess.check_call(['pip3', 'install', 'symengine'])

pathProv = pathlib.Path(__file__).parent.resolve()
# Expand expression
fileProv = open(os.path.join(pathProv,'prov.txt'), 'r')
expression = fileProv.readlines()[0]
fileProv.close()

expanded_expression = symengine.expand(expression)
pattern = r"\*\*\d+"
# Remove the pattern from the text
result = re.sub(pattern, '', str(expanded_expression))

result = result.replace(' + ', '}, {')
result = result.replace('*', ',')
result = re.sub(r"\{\d+,\s*", "{", result)

print('{'+result+'}')
