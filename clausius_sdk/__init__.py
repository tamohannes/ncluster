"""Top-level import shim for the Clausius SDK source tree.

The SDK source lives in ``sdk/`` so it can be vendored into experiment
checkouts as a standalone ``clausius_sdk`` package. This shim makes
``import clausius_sdk`` work directly from the Clausius repository too.
"""

from pathlib import Path

_SDK_DIR = Path(__file__).resolve().parent.parent / "sdk"
__path__ = [str(_SDK_DIR), *list(__path__)]

from clausius_sdk.run import Run  # noqa: E402
from clausius_sdk.session import ClausiusSession  # noqa: E402

__all__ = ["ClausiusSession", "Run"]
