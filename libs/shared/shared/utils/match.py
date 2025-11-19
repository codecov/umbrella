import re
from collections.abc import Sequence


class Matcher:
    def __init__(self, patterns: Sequence[str] | None):
        self._patterns = set(patterns or [])
        self._is_initialized = False
        self._combined_positives: re.Pattern | None = None
        self._combined_negatives: re.Pattern | None = None

    def _get_matchers(self) -> tuple[re.Pattern | None, re.Pattern | None]:
        if not self._is_initialized:
            positive_patterns = []
            negative_patterns = []
            for pattern in self._patterns:
                if not pattern:
                    continue
                if pattern.startswith(("^!", "!")):
                    negative_pattern = pattern.replace("!", "")
                    # self._negatives.append(re.compile(negative_pattern))
                    negative_patterns.append(negative_pattern)
                else:
                    # self._positives.append(re.compile(pattern))
                    positive_patterns.append(pattern)

            # Combine positive patterns into a single regex for faster matching
            if len(positive_patterns) > 0:
                # Combine patterns with OR, each pattern is wrapped in parentheses to preserve anchors
                combined_pattern = (
                    "|".join(f"({p})" for p in positive_patterns)
                    if len(positive_patterns) > 1
                    else positive_patterns[0]
                )
                self._combined_positives = re.compile(combined_pattern)

            # Combine negative patterns into a single regex for faster matching
            if len(negative_patterns) > 0:
                # Combine patterns with OR, each pattern is wrapped in parentheses to preserve anchors
                combined_pattern = (
                    "|".join(f"({p})" for p in negative_patterns)
                    if len(negative_patterns) > 1
                    else negative_patterns[0]
                )
                self._combined_negatives = re.compile(combined_pattern)

            self._is_initialized = True

        return self._combined_positives, self._combined_negatives

    def match(self, s: str) -> bool:
        if not self._patterns or s in self._patterns:
            return True

        combined_positives, combined_negatives = self._get_matchers()

        # Check negatives first - if any match, return False
        if combined_negatives:
            if combined_negatives.match(s):
                return False

        # Check positives - if any match, return True; if none match, return False
        if combined_positives:
            return bool(combined_positives.match(s))

        # No positives: everything else is ok
        return True

    def match_any(self, strings: Sequence[str] | None) -> bool:
        if not strings:
            return False
        return any(self.match(s) for s in strings)


def match(patterns: Sequence[str] | None, string: str):
    matcher = Matcher(patterns)
    return matcher.match(string)


def match_any(
    patterns: Sequence[str] | None, match_any_of_these: Sequence[str] | None
) -> bool:
    matcher = Matcher(patterns)
    return matcher.match_any(match_any_of_these)
