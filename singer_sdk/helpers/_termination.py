"""Internal control-flow signal for graceful target termination.

This module is private to the SDK. It exists so that :class:`singer_sdk.Target`
can interrupt a blocking read of standard input from a signal handler without
performing any sink work inside that handler. The exception is caught at the
input boundary, where a graceful shutdown can run on the normal call stack.
"""

from __future__ import annotations

__all__ = ["TerminationRequested"]


class TerminationRequested(BaseException):
    """A termination signal was received while the target was reading input.

    Inherits from :class:`BaseException` so that broad ``except Exception``
    handlers in user code cannot accidentally swallow a shutdown request.
    """

    def __init__(self, signum: int | None = None) -> None:
        """Initialize the signal.

        Args:
            signum: The signal number that triggered the request, if any.
        """
        self.signum = signum
        super().__init__(f"Termination requested (signal {signum})")
