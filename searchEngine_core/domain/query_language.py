import re
from typing import Dict, List, Optional


BOOLEAN_OPERATOR_PRECEDENCE = {
    "OR": 1,
    "AND": 2,
    "NOT": 3,
}


def extract_quoted_phrase(query: str) -> Optional[str]:
    match = re.search(r'"([^"]+)"', query)
    if not match:
        return None
    return match.group(1).strip()


def _tokenize_operator_query(query: str) -> Optional[List[str]]:
    tokens: List[str] = []
    buffer: List[str] = []
    in_quotes = False
    index = 0
    patterns = [(" NOT ", "NOT"), (" AND ", "AND"), (" OR ", "OR")]

    while index < len(query):
        char = query[index]
        if char == '"':
            in_quotes = not in_quotes
            buffer.append(char)
            index += 1
            continue

        if not in_quotes:
            matched_operator = False
            for pattern, operator in patterns:
                if query[index:].upper().startswith(pattern):
                    operand = "".join(buffer).strip()
                    if not operand:
                        return None
                    tokens.append(operand)
                    tokens.append(operator)
                    buffer = []
                    index += len(pattern)
                    matched_operator = True
                    break

            if matched_operator:
                continue

        buffer.append(char)
        index += 1

    if in_quotes:
        return None

    trailing_operand = "".join(buffer).strip()
    if not trailing_operand:
        return None

    tokens.append(trailing_operand)
    return tokens


def parse_query_with_operators(query: str) -> Optional[Dict]:
    tokens = _tokenize_operator_query(query)
    if not tokens:
        return None

    operator_count = (len(tokens) - 1) // 2
    if operator_count > 2:
        return None
    if operator_count == 0:
        return None

    operators = tokens[1::2]
    operands = tokens[0::2]
    if not all(operands):
        return None

    return {
        "tokens": tokens,
        "operators": operators,
        "count": operator_count,
    }


def count_boolean_operators(query: str) -> int:
    tokens = _tokenize_operator_query(query)
    if not tokens:
        return 0

    return sum(1 for token in tokens if token in BOOLEAN_OPERATOR_PRECEDENCE)


def extract_query_terms(query: str) -> List[str]:
    quoted_phrases = [match.group(1).strip() for match in re.finditer(r'"([^"]+)"', query) if match.group(1).strip()]
    query_without_phrases = re.sub(r'"[^"]+"', " ", query)
    unquoted_words = re.findall(r"[A-Za-z0-9]+", query_without_phrases)

    raw_terms: List[str] = []
    for phrase in quoted_phrases:
        raw_terms.append(phrase)
        raw_terms.extend(re.findall(r"[A-Za-z0-9]+", phrase))
    raw_terms.extend(unquoted_words)

    terms: List[str] = []
    seen = set()
    for term in raw_terms:
        normalized = term.strip()
        if not normalized:
            continue
        if normalized.upper() in BOOLEAN_OPERATOR_PRECEDENCE:
            continue

        key = normalized.lower()
        if key in seen:
            continue

        seen.add(key)
        terms.append(normalized)

    return terms


def to_postfix(tokens: List[str]) -> List[str]:
    postfix: List[str] = []
    operators: List[str] = []

    for token in tokens:
        if token in BOOLEAN_OPERATOR_PRECEDENCE:
            while operators and BOOLEAN_OPERATOR_PRECEDENCE[operators[-1]] >= BOOLEAN_OPERATOR_PRECEDENCE[token]:
                postfix.append(operators.pop())
            operators.append(token)
        else:
            postfix.append(token)

    while operators:
        postfix.append(operators.pop())

    return postfix