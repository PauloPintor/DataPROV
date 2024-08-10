import sys
import os
import subprocess
import re

try:
    # Try importing SymEngine
    import symengine
except ImportError:
    # If SymEngine is not installed, install it
    subprocess.check_call(['pip3', 'install', 'symengine'])

def main():
    # Check if the argument is passed
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        else:
            fileProv = open(file_path, 'r')
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
    else:
        raise ValueError("No variable received from Java")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Exception: {e}")
        sys.exit(1) 