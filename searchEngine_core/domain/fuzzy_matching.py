from typing import List, Sequence, Tuple


def max_fuzzy_distance(term: str) -> int:
    if len(term) <= 4:
        return 1
    if len(term) <= 8:
        return 2
    return 3


def fuzzy_match_weight(distance: int) -> float:
    return max(0.35, 0.75 - (0.15 * max(distance - 1, 0)))


def bounded_edit_distance(source: str, target: str, max_distance: int) -> int:
    if source == target:
        return 0

    if abs(len(source) - len(target)) > max_distance:
        return max_distance + 1

    if len(source) > len(target):
        source, target = target, source

    previous_previous: List[int] | None = None
    previous = list(range(len(target) + 1))

    for row_index, source_char in enumerate(source, start=1):
        current = [row_index]
        row_min = current[0]

        for column_index, target_char in enumerate(target, start=1):
            insert_cost = current[column_index - 1] + 1
            delete_cost = previous[column_index] + 1
            replace_cost = previous[column_index - 1] + (source_char != target_char)
            best_cost = min(insert_cost, delete_cost, replace_cost)

            if (
                previous_previous is not None
                and row_index > 1
                and column_index > 1
                and source[row_index - 1] == target[column_index - 2]
                and source[row_index - 2] == target[column_index - 1]
            ):
                best_cost = min(best_cost, previous_previous[column_index - 2] + 1)

            current.append(best_cost)
            row_min = min(row_min, best_cost)

        if row_min > max_distance:
            return max_distance + 1

        previous_previous, previous = previous, current

    return previous[-1]


def find_fuzzy_matches(
    term: str,
    candidates: Sequence[str],
    max_distance: int | None = None,
    max_expansions: int = 3,
    min_term_length: int = 3,
) -> List[Tuple[str, float]]:
    normalized_term = term.strip().lower()
    if len(normalized_term) < min_term_length:
        return []

    allowed_distance = max_distance if max_distance is not None else max_fuzzy_distance(normalized_term)
    matches: List[Tuple[str, float, int, int]] = []
    seen: set[str] = set()

    for candidate in candidates:
        if not isinstance(candidate, str):
            continue

        normalized_candidate = candidate.strip().lower()
        if not normalized_candidate or normalized_candidate in seen or normalized_candidate == normalized_term:
            continue

        if abs(len(normalized_candidate) - len(normalized_term)) > allowed_distance:
            continue

        distance = bounded_edit_distance(normalized_term, normalized_candidate, allowed_distance)
        if distance > allowed_distance:
            continue

        seen.add(normalized_candidate)
        matches.append(
            (
                normalized_candidate,
                fuzzy_match_weight(distance),
                distance,
                abs(len(normalized_candidate) - len(normalized_term)),
            )
        )

    matches.sort(key=lambda item: (item[2], item[3], -item[1], item[0]))
    return [(candidate, weight) for candidate, weight, _, _ in matches[:max_expansions]]
