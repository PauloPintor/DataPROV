import re


def replace_words_with_tokens(s):
    """
    Replace words that match the pattern (one or more uppercase letters followed by one or more digits)
    with tokens (x1, x2, x3, ...). If a word appears more than once, it is replaced with the same token.

    Args:
        s (str): The input string.

    Returns:
        tuple: A tuple containing:
            - The transformed string.
            - A dictionary mapping original words to tokens.
    """
    mapping = {}
    counter = [1]  # Using a list for a mutable integer in the nested function

    # Define the replacement function
    def repl(match):
        word = match.group(0)
        if word not in mapping:
            mapping[word] = f"x{counter[0]}"
            counter[0] += 1
        return mapping[word]

    # Regex pattern to match words (one or more uppercase letters followed by one or more digits)
    pattern = re.compile(r"\b[A-Z]+\d+\b")

    # Replace the words using re.sub with the repl function
    result = pattern.sub(repl, s)
    return result, mapping


def replace_words_with_fixed_number(s, number=1):
    """
    Replace all words matching the pattern (one or more uppercase letters followed by one or more digits)
    with the same fixed number.

    Args:
        s (str): The input string.
        number (int): The number to replace all matches with (default: 1).

    Returns:
        str: The transformed string.
    """
    # Regex pattern to match words (one or more uppercase letters followed by one or more digits)
    pattern = re.compile(r"\b[A-Z]+\d+\b")

    # Replace all matches with the same number
    return pattern.sub(str(number), s)


def replace_tokens_with_fixed_number(expression, number=1):
    return re.sub(r"x\d+", str(number), expression)
