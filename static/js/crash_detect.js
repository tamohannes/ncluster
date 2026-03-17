/* Crash-detection helpers for log-line classification.
 *
 * Shared constants that the log viewer uses to decide whether a line
 * containing "error" / "warning" is actually benign (e.g. sandbox restarts).
 * Keep in sync with server/crash_detect.py.
 */

const BENIGN_LINE_PATTERNS = [
  'sandbox state restoration failed',
  'sandbox communication error',
];

function isBenignLogLine(lineLower) {
  return BENIGN_LINE_PATTERNS.some(p => lineLower.includes(p));
}
