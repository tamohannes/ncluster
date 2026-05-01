/* Crash-detection helpers for log-line classification.
 *
 * Mirrors the patterns from server/crash_detect.py so the log viewer
 * highlights only real errors (tracebacks, typed exceptions, CUDA/OOM,
 * srun failures) instead of any line containing the word "error".
 */

const BENIGN_LINE_PATTERNS = [
  'sandbox state restoration failed',
  'sandbox communication error',
  'sending tool calls:',
];

const ERROR_LINE_PATTERNS = [
  /Traceback \(most recent call last\)/i,
  /^(Type|Value|Runtime|Key|Attribute|Import|Module|Index|FileNotFound|OS|Permission|IO|Lookup|Assertion|ZeroDivision|Overflow|Memory|NotImplemented)Error:/m,
  /CUDA (error|out of memory)/i,
  /srun: error:/i,
  /Killed\s*$/,
  /\bOOM\b|Out of memory/i,
  /^Error executing job/im,
];

function isBenignLogLine(lineLower) {
  return BENIGN_LINE_PATTERNS.some(p => lineLower.includes(p));
}

function isErrorLogLine(line) {
  return ERROR_LINE_PATTERNS.some(p => p.test(line));
}
