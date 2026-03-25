"""Crash detection and log-line classification.

Scans log content for fatal error patterns while filtering out known
non-fatal messages (sandbox restarts, transient communication errors, etc.).
"""

import re

# ── Fatal patterns ────────────────────────────────────────────────────────────
# Each regex is tested against the tail of a log file.  First match wins.

CRASH_PATTERNS = [
    re.compile(r'Traceback \(most recent call last\)', re.I),
    re.compile(
        r'^(Type|Value|Runtime|Key|Attribute|Import|Module|Index|FileNotFound'
        r'|OS|Permission|IO|Lookup|Assertion|ZeroDivision|Overflow|Memory'
        r'|NotImplemented)Error:',
        re.M,
    ),
    re.compile(r'CUDA (error|out of memory)', re.I),
    re.compile(r'srun: error:', re.I),
    re.compile(r'Killed\s*$', re.M),
    re.compile(r'OOM|Out of memory', re.I),
    re.compile(r'Error executing job', re.I),
]

# ── False-positive filters ────────────────────────────────────────────────────
# Lines matching these are stripped before crash-pattern search so that
# embedded tracebacks / error names inside non-fatal messages don't trigger
# false alarms.

FALSE_POSITIVE_PATTERNS = [
    re.compile(r'Sandbox state restoration failed', re.I),
    re.compile(r'Sandbox communication error', re.I),
]

# Client-side counterpart: substrings (lowercase) that make a log line
# "benign" — the UI should show them as warnings, not errors.
BENIGN_LINE_SUBSTRINGS = [
    'sandbox state restoration failed',
    'sandbox communication error',
]

# ── Tail size used by detect_crash ────────────────────────────────────────────

TAIL_BYTES = 8192


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_false_positives(text):
    """Remove lines containing known non-fatal messages that embed error-like strings."""
    return '\n'.join(
        line for line in text.split('\n')
        if not any(fp.search(line) for fp in FALSE_POSITIVE_PATTERNS)
    )


def detect_crash(content):
    """Return a short crash reason if error patterns found in log tail, else None."""
    if not content:
        return None
    tail = _strip_false_positives(content[-TAIL_BYTES:])
    for pat in CRASH_PATTERNS:
        m = pat.search(tail)
        if m:
            return m.group(0)[:80]
    return None


# ── Soft-failure indicators ───────────────────────────────────────────────────
# When a retry/continuation job finds all work already completed, the main
# generation step skips successfully but downstream steps (e.g. eval) crash
# because no output files were produced.  These patterns indicate the *main
# work* was intentionally skipped — the crash is collateral, not a real failure.

SOFT_FAIL_INDICATORS = [
    re.compile(r'No data to process', re.I),
    re.compile(r'exists,?\s+skipping', re.I),
    re.compile(r'nothing\s+to\s+(process|evaluate|generate)', re.I),
    re.compile(r'all\b.*\balready\s+(completed|processed|evaluated|done)', re.I),
    re.compile(r'0\s+samples?\s+to\s+process', re.I),
]


def detect_soft_failure(content):
    """Return a short reason if the failure is a no-op (work already done), else None.

    Soft failures occur when retry/continuation jobs find all work already
    completed and exit non-zero because downstream steps (e.g. evaluation)
    have no output files to process.
    """
    if not content:
        return None
    for pat in SOFT_FAIL_INDICATORS:
        m = pat.search(content)
        if m:
            return m.group(0)[:80]
    return None


def is_benign_line(line_lower):
    """Return True if *line_lower* (already lowercased) matches a benign pattern."""
    return any(sub in line_lower for sub in BENIGN_LINE_SUBSTRINGS)
