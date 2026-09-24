import fnmatch
import re


def _safe_match(pattern: str, string: str) -> re.Match | None:
    try:
        return re.match(pattern, string)
    except re.error:
        return re.match(fnmatch.translate(pattern), string)


def match(patterns: list[str] | None, string: str) -> bool:
    if patterns is None or string in patterns:
        return True

    patterns = set(filter(None, patterns))
    negatives = set(filter(lambda a: a.startswith(("^!", "!")), patterns))
    positives = patterns - negatives

    # must not match
    for pattern in negatives:
        # matched a negative search
        if _safe_match(pattern.replace("!", ""), string):
            return False

    if positives:
        for pattern in positives:
            # match was found
            if _safe_match(pattern, string):
                return True

        # did not match any required paths
        return False

    else:
        # no positives: everyting else is ok
        return True
