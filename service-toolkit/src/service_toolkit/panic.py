from typing import Any
import json
import sys
import traceback

def log(level: str, message: str, **fields: Any):
    print(
        json.dumps(
            {
                "level": level,
                "message": message,
                **fields,
            }
        ),
        file=sys.stderr,
    )


def _handle_exception(exc_type, exc_value, exc_traceback):
    log(
        "CRITICAL",
        "unhandled exception",
        unhandled=True,
        trace="".join(traceback.format_exception(exc_value)),
    )
    exit(1)


def set_excepthook():
    sys.excepthook = _handle_exception
